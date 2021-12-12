package com.the_qa_company.q_endpoint.hybridstore;

import com.the_qa_company.q_endpoint.model.AbstractValueFactoryHDT;
import com.the_qa_company.q_endpoint.model.SimpleIRIHDT;
import com.the_qa_company.q_endpoint.utils.CombinedNativeStoreResult;
import com.the_qa_company.q_endpoint.utils.HDTConverter;
import com.the_qa_company.q_endpoint.utils.IRIConverter;
import com.the_qa_company.q_endpoint.utils.TripleWithDeleteIter;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.memory.model.MemValueFactory;
import org.rdfhdt.hdt.enums.TripleComponentOrder;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.impl.EmptyTriplesIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// this is the main class telling how, given a triple pattern, to find the results in HDT and the current stores
public class HybridTripleSource implements TripleSource {
    private static final Logger logger = LoggerFactory.getLogger(HybridTripleSource.class);
    private final HybridStore hybridStore;
    ValueFactory factory;
    ValueFactory tempFactory;
    private long numberOfCurrentTriples;
    // count the number of times rdf4j is called within a triple pattern..
    // only for debugging ...
    private long count = 0;
    HybridStoreConnection hybridStoreConnection;

    public HybridTripleSource(HybridStoreConnection hybridStoreConnection, HDT hdt, HybridStore hybridStore) {
        this.hybridStore = hybridStore;
        this.factory = new AbstractValueFactoryHDT(hdt);
        this.numberOfCurrentTriples = hdt.getTriples().getNumberOfElements();
        this.tempFactory = new MemValueFactory();
        this.hybridStoreConnection = hybridStoreConnection;
    }

    public ValueFactory getTempFactory() {
        return tempFactory;
    }

    private void initHDTIndex() {
        this.numberOfCurrentTriples = this.hybridStore.getHdt().getTriples().getNumberOfElements();
    }

    @Override
    public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(
            Resource resource, IRI iri, Value value, Resource... resources)
            throws QueryEvaluationException {

        // @todo: should we not move this to the HybridStore in the resetHDT function?
        // check if the index changed, then refresh it
        if (this.numberOfCurrentTriples != this.hybridStore.getHdt().getTriples().getNumberOfElements()) {
            initHDTIndex();
        }

        // convert uris into ids if needed
        Resource newRes = hybridStore.getIriConverter().convertSubj(resource);
        IRI newIRI = hybridStore.getIriConverter().convertPred(iri);
        Value newValue = hybridStore.getIriConverter().convertObj(value);

        // check if we need to search over the delta and if yes, search
        CloseableIteration<? extends Statement, SailException> repositoryResult = null;
        if (shouldSearchOverNativeStore(newRes, newIRI, newValue)) {
            count++;

            if (hybridStore.isMerging()) {
                // query both native stores
                CloseableIteration<? extends Statement, SailException> repositoryResult1 =
                        this.hybridStoreConnection.getConnA().getStatements(
                                newRes, newIRI, newValue, false, resources
                        );
                CloseableIteration<? extends Statement, SailException> repositoryResult2 =
                        this.hybridStoreConnection.getConnB().getStatements(
                                newRes, newIRI, newValue, false, resources
                        );
                repositoryResult = new CombinedNativeStoreResult(repositoryResult1, repositoryResult2);

            } else {
                repositoryResult = this.hybridStoreConnection.getCurrentConnection().getStatements(
                        newRes, newIRI, newValue, false, resources
                );
            }
        }

        // @todo: does this not happen already above in iriConverter.convertSubj ?
        long subject = hybridStore.getHdtConverter().subjectId(resource);
        long predicate = hybridStore.getHdtConverter().predicateId(iri);
        long object = hybridStore.getHdtConverter().objectId(value);
        if (logger.isDebugEnabled()) {
            if (resource != null) {
                logger.debug(resource.toString());
            }
            if (iri != null) {
                logger.debug(iri.toString());
            }
            if (value != null) {
                logger.debug(value.stringValue());
            }
            logger.debug(subject + "  " + predicate + "  " + object);
        }

        // iterate over the HDT file
        IteratorTripleID iterator;
        if (subject != -1 && predicate != -1 && object != -1) {
            TripleID t = new TripleID(subject, predicate, object);
            // search with the ID to check if the triples has been deleted
            iterator = this.hybridStore.getHdt().getTriples().searchWithId(t);

        } else {// no need to search over hdt
            iterator = new EmptyTriplesIterator(TripleComponentOrder.SPO);
        }

        // iterate over hdt result, delete the triples marked as deleted and add the triples from the delta
        TripleWithDeleteIter tripleWithDeleteIter = new TripleWithDeleteIter(hybridStore, this, iterator, repositoryResult);
        return new CloseableIteration<>() {
            @Override
            public void close() throws QueryEvaluationException {
            }

            @Override
            public boolean hasNext() throws QueryEvaluationException {
                return tripleWithDeleteIter.hasNext();
            }

            @Override
            public Statement next() throws QueryEvaluationException {
                Statement stm = tripleWithDeleteIter.next();
                return stm;
            }

            @Override
            public void remove() throws QueryEvaluationException {
                tripleWithDeleteIter.remove();
            }
        };
    }

    // this function determines if a triple pattern should be searched over the native store. This is only
    // the case if the subject, predicate and object were marked as used in the bitmaps
    private boolean shouldSearchOverNativeStore(Resource subject, IRI predicate, Value object) {
        boolean containsSubject = false;
        boolean containsPredicate = false;
        boolean containsObject = false;

        if (subject instanceof SimpleIRIHDT) {
            if (((SimpleIRIHDT) subject).getId() != -1)
                containsSubject = this.hybridStore.getBitX().access(((SimpleIRIHDT) subject).getId() - 1);
            else
                containsSubject = true;
        } else {
            containsSubject = true;
        }
        if (predicate instanceof SimpleIRIHDT) {
            if (((SimpleIRIHDT) predicate).getId() != -1)
                containsPredicate = this.hybridStore.getBitY().access(((SimpleIRIHDT) predicate).getId() - 1);
            else
                containsPredicate = true;
        } else {
            containsPredicate = true;
        }
        if (object instanceof SimpleIRIHDT) {
            if (((SimpleIRIHDT) object).getId() != -1) {
                if (((SimpleIRIHDT) object).getPostion() == SimpleIRIHDT.SHARED_POS)
                    containsObject = this.hybridStore.getBitX().access(((SimpleIRIHDT) object).getId() - 1);
                else if (((SimpleIRIHDT) object).getPostion() == SimpleIRIHDT.OBJECT_POS)
                    containsObject = this.hybridStore.getBitZ().access(((SimpleIRIHDT) object).getId() - 1);
            } else
                containsObject = true;
        } else {
            containsObject = true;
        }
        return containsSubject && containsPredicate && containsObject;
    }

    @Override
    public ValueFactory getValueFactory() {
        return factory;
    }

    public HybridStore getHybridStore() {
        return hybridStore;
    }

    public long getCount() {
        return count;
    }
}
