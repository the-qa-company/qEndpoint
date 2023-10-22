package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.store.exception.EndpointTimeoutException;
import com.the_qa_company.qendpoint.utils.CombinedNativeStoreResult;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.sail.SailException;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.impl.EmptyTriplesIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// this is the main class telling how, given a triple pattern, to find the results in HDT and the current stores
public class EndpointTripleSource implements TripleSource {
	private static final Logger logger = LoggerFactory.getLogger(EndpointTripleSource.class);
	private final EndpointStore endpoint;
	private long numberOfCurrentTriples;
	// count the number of times rdf4j is called within a triple pattern..
	// only for debugging ...
	private long count = 0;
	private final EndpointStoreConnection endpointStoreConnection;

	public EndpointTripleSource(EndpointStoreConnection endpointStoreConnection, EndpointStore endpoint) {
		this.endpoint = endpoint;
		this.numberOfCurrentTriples = endpoint.getHdt().getTriples().getNumberOfElements();
		this.endpointStoreConnection = endpointStoreConnection;
	}

	private void initHDTIndex() {
		this.numberOfCurrentTriples = this.endpoint.getHdt().getTriples().getNumberOfElements();
	}

	@Override
	public CloseableIteration<? extends Statement> getStatements(Resource resource, IRI iri,
			Value value, Resource... resources) throws QueryEvaluationException {

		if (EndpointStoreConnection.debugWaittime != 0) {
			try {
				Thread.sleep(EndpointStoreConnection.debugWaittime);
			} catch (InterruptedException e) {
				throw new AssertionError("no interruption during sleep", e);
			}
		}

		if (endpointStoreConnection.isTimeout()) {
			throw new EndpointTimeoutException();
		}

		// @todo: should we not move this to the EndpointStore in the resetHDT
		// function?
		// check if the index changed, then refresh it
		if (this.numberOfCurrentTriples != this.endpoint.getHdt().getTriples().getNumberOfElements()) {
			initHDTIndex();
		}

		// convert uris into ids if needed
		Resource newSubj;
		IRI newPred;
		Value newObj;
		long subjectID = this.endpoint.getHdtConverter().subjectToID(resource);
		long predicateID = this.endpoint.getHdtConverter().predicateToID(iri);
		long objectID = this.endpoint.getHdtConverter().objectToID(value);

		if (subjectID == 0 || subjectID == -1) {
			newSubj = resource;
		} else {
			newSubj = this.endpoint.getHdtConverter().subjectIdToIRI(subjectID);
		}
		if (predicateID == 0 || predicateID == -1) {
			newPred = iri;
		} else {
			newPred = this.endpoint.getHdtConverter().predicateIdToIRI(predicateID);
		}
		if (objectID == 0 || objectID == -1) {
			newObj = value;
		} else {
			newObj = this.endpoint.getHdtConverter().objectIdToIRI(objectID);
		}

		logger.debug("SEARCH {} {} {}", newSubj, newPred, newObj);

		// check if we need to search over the delta and if yes, search
		CloseableIteration<? extends Statement> repositoryResult;
		if (shouldSearchOverNativeStore(subjectID, predicateID, objectID)) {
			logger.debug("Searching over native store");
			count++;
			if (endpoint.isMergeTriggered) {
				// query both native stores
				logger.debug("Query both RDF4j stores!");
				CloseableIteration<? extends Statement> repositoryResult1 = this.endpointStoreConnection
						.getConnA_read().getStatements(newSubj, newPred, newObj, false, resources);
				CloseableIteration<? extends Statement> repositoryResult2 = this.endpointStoreConnection
						.getConnB_read().getStatements(newSubj, newPred, newObj, false, resources);
				repositoryResult = new CombinedNativeStoreResult(repositoryResult1, repositoryResult2);

			} else {
				logger.debug("Query only one RDF4j stores!");
				repositoryResult = this.endpointStoreConnection.getCurrentConnectionRead().getStatements(newSubj,
						newPred, newObj, false, resources);
			}
		} else {
			logger.debug("Not searching over native store");
			repositoryResult = new EmptyIteration<>();
		}

		// iterate over the HDT file
		IteratorTripleID iterator;
		if (subjectID != -1 && predicateID != -1 && objectID != -1) {
			logger.debug("Searching over HDT {} {} {}", subjectID, predicateID, objectID);
			TripleID t = new TripleID(subjectID, predicateID, objectID);
			// search with the ID to check if the triples has been deleted
			iterator = this.endpoint.getHdt().getTriples().search(t);
		} else {// no need to search over hdt
			iterator = new EmptyTriplesIterator(TripleComponentOrder.SPO);
		}

		// iterate over hdt result, delete the triples marked as deleted and add
		// the triples from the delta
		return new EndpointStoreTripleIterator(endpointStoreConnection, this, iterator, repositoryResult);
	}

	// this function determines if a triple pattern should be searched over the
	// native store. This is only
	// the case if the subject, predicate and object were marked as used in the
	// bitmaps
	private boolean shouldSearchOverNativeStore(long subject, long predicate, long object) {
		if (logger.isDebugEnabled()) {
			boolean containsSubject = true;
			boolean containsPredicate = true;
			boolean containsObject = true;

			if (subject != 0 && subject != -1) {
				containsSubject = this.endpoint.getBitX().access(subject - 1);
			}
			if (predicate != 0 && predicate != -1) {
				containsPredicate = this.endpoint.getBitY().access(predicate - 1);
			}
			if (object != 0 && object != -1) {
				if (object <= this.endpoint.getHdt().getDictionary().getNshared()) {
					containsObject = this.endpoint.getBitX().access(object - 1);
				} else {
					containsObject = this.endpoint.getBitZ()
							.access(object - this.endpoint.getHdt().getDictionary().getNshared() - 1);
				}
			}
			logger.debug("Search over native store? {} {} {}", containsSubject, containsPredicate, containsObject);
			return containsSubject && containsPredicate && containsObject;
		} else {
			if (subject != 0 && subject != -1 && !this.endpoint.getBitX().access(subject - 1)) {
				return false;
			}
			if (predicate != 0 && predicate != -1 && !this.endpoint.getBitY().access(predicate - 1)) {
				return false;
			}
			if (object != 0 && object != -1) {
				if (object <= this.endpoint.getHdt().getDictionary().getNshared()) {
					return this.endpoint.getBitX().access(object - 1);
				} else {
					return this.endpoint.getBitZ()
							.access(object - this.endpoint.getHdt().getDictionary().getNshared() - 1);
				}
			}
		}
		return true;
	}

	@Override
	public ValueFactory getValueFactory() {
		return endpoint.getValueFactory();
	}

	public EndpointStore getEndpointStore() {
		return endpoint;
	}

	public long getCount() {
		return count;
	}
}
