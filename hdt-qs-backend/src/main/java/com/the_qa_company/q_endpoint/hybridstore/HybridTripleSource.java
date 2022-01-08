package com.the_qa_company.q_endpoint.hybridstore;

import com.the_qa_company.q_endpoint.utils.CombinedNativeStoreResult;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.sail.SailException;
import org.rdfhdt.hdt.enums.TripleComponentOrder;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.impl.EmptyTriplesIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// this is the main class telling how, given a triple pattern, to find the results in HDT and the current stores
public class HybridTripleSource implements TripleSource {
    private static final Logger logger = LoggerFactory.getLogger(HybridTripleSource.class);
    private final HybridStore hybridStore;
    ValueFactory tempFactory;
    private long numberOfCurrentTriples;
    // count the number of times rdf4j is called within a triple pattern..
    // only for debugging ...
    private long count = 0;
    HybridStoreConnection hybridStoreConnection;

    public HybridTripleSource(HybridStoreConnection hybridStoreConnection, HybridStore hybridStore) {
        this.hybridStore = hybridStore;
        this.numberOfCurrentTriples = hybridStore.getHdt().getTriples().getNumberOfElements();
        this.hybridStoreConnection = hybridStoreConnection;
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
        Resource newSubj;
        IRI newPred;
        Value newObj;
        long subjectID = this.hybridStore.getHdtConverter().subjectToID(resource);
        long predicateID = this.hybridStore.getHdtConverter().predicateToID(iri);
        long objectID = this.hybridStore.getHdtConverter().objectToID(value);


        if (subjectID == 0 || subjectID == -1){
            newSubj = resource;
        } else {
            newSubj = this.hybridStore.getHdtConverter().subjectIdToIRI(subjectID);
        }
        if (predicateID == 0 || predicateID == -1){
            newPred = iri;
        } else {
            newPred = this.hybridStore.getHdtConverter().predicateIdToIRI(predicateID);
        }
        if (objectID == 0 || objectID == -1){
            newObj = value;
        } else {
            newObj = this.hybridStore.getHdtConverter().objectIdToIRI(objectID);
        }

        logger.debug("SEARCH "+newSubj+" - "+ newPred + " - " + newObj);


        // check if we need to search over the delta and if yes, search
        CloseableIteration<? extends Statement, SailException> repositoryResult = null;
        if (shouldSearchOverNativeStore(subjectID, predicateID, objectID)) {
            logger.debug("Searching over native store");
            count++;
            if (hybridStore.isMergeTriggered) {
                // query both native stores
                logger.debug("Query both RDF4j stores!");
                CloseableIteration<? extends Statement, SailException> repositoryResult1 =
                        this.hybridStoreConnection.getConnA().getStatements(
                                newSubj, newPred, newObj, false, resources
                        );
                CloseableIteration<? extends Statement, SailException> repositoryResult2 =
                        this.hybridStoreConnection.getConnB().getStatements(
                                newSubj, newPred, newObj, false, resources
                        );
                repositoryResult = new CombinedNativeStoreResult(repositoryResult1, repositoryResult2);

            } else {
                logger.debug("Query only one RDF4j stores!");
                repositoryResult = this.hybridStoreConnection.getCurrentConnection().getStatements(
                        newSubj, newPred, newObj, false, resources
                );
            }
        } else {
            logger.debug("Not searching over native store");
        }

        // iterate over the HDT file
        IteratorTripleID iterator;
        if ( subjectID != -1 && predicateID != -1 && objectID != -1) {
            logger.debug("Searching over HDT {} {} {}",subjectID, predicateID, objectID);
            TripleID t = new TripleID(subjectID, predicateID, objectID);
            // search with the ID to check if the triples has been deleted
            iterator = this.hybridStore.getHdt().getTriples().searchWithId(t);
        } else {// no need to search over hdt
            iterator = new EmptyTriplesIterator(TripleComponentOrder.SPO);
        }

        // iterate over hdt result, delete the triples marked as deleted and add the triples from the delta
        HybridStoreTripleIterator tripleWithDeleteIter = new HybridStoreTripleIterator(hybridStore, this, iterator, repositoryResult);
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
    private boolean shouldSearchOverNativeStore(long subject, long predicate, long object) {
        boolean containsSubject = true;
        boolean containsPredicate = true;
        boolean containsObject = true;

        if (subject != 0 && subject != -1) {
            containsSubject = this.hybridStore.getBitX().access(subject - 1);
        }
        if (predicate != 0 && predicate != -1) {
            containsPredicate = this.hybridStore.getBitY().access(predicate - 1);
        }
        if (object!= 0 && object != -1) {
            if (object <= this.hybridStore.getHdt().getDictionary().getNshared()) {
                containsObject = this.hybridStore.getBitX().access(object - 1);
            } else {
                containsObject = this.hybridStore.getBitZ().access(object - - this.hybridStore.getHdt().getDictionary().getNshared() - 1);
            }
        }
        logger.debug("Search over native store? {} {} {}",containsSubject, containsPredicate, containsObject);
        return containsSubject && containsPredicate && containsObject;
    }

    @Override
    public ValueFactory getValueFactory() {
        return hybridStore.getValueFactory();
    }

    public HybridStore getHybridStore() {
        return hybridStore;
    }

    public long getCount() {
        return count;
    }
}
