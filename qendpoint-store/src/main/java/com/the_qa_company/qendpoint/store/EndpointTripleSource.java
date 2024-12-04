package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.iterator.utils.GraphFilteringTripleId;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.impl.EmptyTriplesIterator;
import com.the_qa_company.qendpoint.model.HDTValue;
import com.the_qa_company.qendpoint.store.exception.EndpointTimeoutException;
import com.the_qa_company.qendpoint.utils.CombinedNativeStoreResult;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.order.StatementOrder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

// this is the main class telling how, given a triple pattern, to find the results in HDT and the current stores
public class EndpointTripleSource implements TripleSource {

	private static final Logger logger = LoggerFactory.getLogger(EndpointTripleSource.class);
	private final EndpointStore endpoint;
	private long numberOfCurrentTriples;
	// count the number of times rdf4j is called within a triple pattern..
	// only for debugging ...
	private long count = 0;
	private final EndpointStoreConnection endpointStoreConnection;
	private final boolean enableMergeJoin;

	public EndpointTripleSource(EndpointStoreConnection endpointStoreConnection, EndpointStore endpoint) {
		this.endpoint = endpoint;
		this.numberOfCurrentTriples = endpoint.getHdt().getTriples().getNumberOfElements();
		this.endpointStoreConnection = endpointStoreConnection;
		this.enableMergeJoin = endpoint.getHDTSpec().getBoolean(EndpointStore.OPTION_QENDPOINT_MERGE_JOIN, false);
	}

	public boolean hasEnableMergeJoin() {
		return enableMergeJoin;
	}

	private void initHDTIndex() {
		this.numberOfCurrentTriples = this.endpoint.getHdt().getTriples().getNumberOfElements();
	}

	@Override
	public CloseableIteration<? extends Statement> getStatements(Resource subj, IRI pred, Value obj,
			Resource... contexts) throws QueryEvaluationException {

		return getStatements(null, subj, pred, obj, contexts);

	}

	@Override
	public CloseableIteration<? extends Statement> getStatements(StatementOrder statementOrder, Resource subj, IRI pred,
			Value obj, Resource... contexts) throws SailException {

		if (statementOrder != null && logger.isDebugEnabled()) {
			logger.debug("getStatements(StatementOrder {}, Subject {}, Predicate {}, Object {}, Contexts... {})",
					statementOrder, subj, pred, obj, contexts);
		}

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

		boolean graph = endpoint.getHdt().getDictionary().supportGraphs();

		// convert uris into ids if needed
		Resource newSubj;
		IRI newPred;
		Value newObj;
		Resource[] newContextes;
		long subjectID = this.endpoint.getHdtConverter().subjectToID(subj);
		long predicateID = this.endpoint.getHdtConverter().predicateToID(pred);
		long objectID = this.endpoint.getHdtConverter().objectToID(obj);
		long[] graphID;

		if (subjectID == 0 || subjectID == -1) {
			newSubj = subj;
		} else {
			newSubj = this.endpoint.getHdtConverter().subjectIdToIRI(subjectID);
		}
		if (predicateID == 0 || predicateID == -1) {
			newPred = pred;
		} else {
			newPred = this.endpoint.getHdtConverter().predicateIdToIRI(predicateID);
		}
		if (objectID == 0 || objectID == -1) {
			newObj = obj;
		} else {
			newObj = this.endpoint.getHdtConverter().objectIdToIRI(objectID);
		}

		if (graph) {
			graphID = new long[contexts.length];
			newContextes = this.endpoint.getHdtConverter().graphIdToIRI(contexts, graphID);
		} else {
			graphID = null;
			newContextes = contexts;
		}

		// logger.debug("SEARCH {} {} {}", newSubj, newPred, newObj);

		// check if we need to search over the delta and if yes, search
		CloseableIteration<? extends Statement> repositoryResult;
		if (shouldSearchOverNativeStore(subjectID, predicateID, objectID)) {
			if (statementOrder != null) {
				throw new UnsupportedOperationException(
						"Statement ordering is not supported when searching over the native store");
			}
			logger.debug("Searching over native store");
			count++;
			if (endpoint.isMergeTriggered) {
				// query both native stores
				logger.debug("Query both RDF4j stores!");
				CloseableIteration<? extends Statement> repositoryResult1 = this.endpointStoreConnection.getConnA_read()
						.getStatements(newSubj, newPred, newObj, false, newContextes);
				CloseableIteration<? extends Statement> repositoryResult2 = this.endpointStoreConnection.getConnB_read()
						.getStatements(newSubj, newPred, newObj, false, newContextes);
				repositoryResult = new CombinedNativeStoreResult(repositoryResult1, repositoryResult2);

			} else {
				logger.debug("Query only one RDF4j stores!");
				repositoryResult = this.endpointStoreConnection.getCurrentConnectionRead().getStatements(newSubj,
						newPred, newObj, false, newContextes);
			}
		} else {
			logger.debug("Not searching over native store");
			repositoryResult = new EmptyIteration<>();
		}

		// iterate over the HDT file
		IteratorTripleID iterator;
		if (subjectID != -1 && predicateID != -1 && objectID != -1) {
			// logger.debug("Searching over HDT {} {} {}", subjectID,
			// predicateID, objectID);
			TripleID t = new TripleID(subjectID, predicateID, objectID);

			if (graph && contexts.length > 1) {
				if (statementOrder != null) {
					int indexMaskMatchingStatementOrder = getIndexMaskMatchingStatementOrder(statementOrder, subj, pred,
							obj, t);

					// search with the ID to check if the triples has been
					// deleted
					iterator = new GraphFilteringTripleId(
							this.endpoint.getHdt().getTriples().search(t, indexMaskMatchingStatementOrder), graphID);
				} else {
					// search with the ID to check if the triples has been
					// deleted
					iterator = new GraphFilteringTripleId(this.endpoint.getHdt().getTriples().search(t), graphID);
				}
			} else {
				if (graph && contexts.length == 1) {
					t.setGraph(graphID[0]);
				}
				if (statementOrder != null) {
					int indexMaskMatchingStatementOrder = getIndexMaskMatchingStatementOrder(statementOrder, subj, pred,
							obj, t);

					// search with the ID to check if the triples has been
					// deleted
					iterator = this.endpoint.getHdt().getTriples().search(t, indexMaskMatchingStatementOrder);
				} else {
					// search with the ID to check if the triples has been
					// deleted
					iterator = this.endpoint.getHdt().getTriples().search(t);
				}
			}

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

	private int getIndexMaskMatchingStatementOrder(StatementOrder statementOrder, Resource subj, IRI pred, Value obj,
			TripleID t) {
		List<TripleComponentOrder> tripleComponentOrder = this.endpoint.getHdt().getTriples()
				.getTripleComponentOrder(t);

		if (subj != null && pred != null && obj != null) {
			if (!tripleComponentOrder.isEmpty()) {
				return tripleComponentOrder.get(0).mask;
			}
		}

		Optional<TripleComponentOrder> first = tripleComponentOrder.stream()
				.filter(o -> getStatementOrder(o, subj != null, pred != null, obj != null).contains(statementOrder))
				.findFirst();

		if (first.isEmpty()) {
			throw new AssertionError(
					"Statement order " + statementOrder + " not supported for triple pattern " + t.getPatternString());
		}
		return first.get().mask;
	}

	public static Set<StatementOrder> getStatementOrder(TripleComponentOrder tripleComponentOrder, boolean subject,
			boolean predicate, boolean object) {
		List<TripleComponentRole> subjectMappings = List.of(tripleComponentOrder.getSubjectMapping(),
				tripleComponentOrder.getPredicateMapping(), tripleComponentOrder.getObjectMapping());

		EnumSet<StatementOrder> statementOrders = EnumSet.noneOf(StatementOrder.class);
		if (subject) {
			statementOrders.add(StatementOrder.S);
		}
		if (predicate) {
			statementOrders.add(StatementOrder.P);
		}
		if (object) {
			statementOrders.add(StatementOrder.O);
		}

		for (TripleComponentRole mapping : subjectMappings) {
			if (mapping == TripleComponentRole.SUBJECT) {
				if (!subject) {
					statementOrders.add(StatementOrder.S);
					break;
				}
			} else if (mapping == TripleComponentRole.PREDICATE) {
				if (!predicate) {
					statementOrders.add(StatementOrder.P);
					break;
				}
			} else if (mapping == TripleComponentRole.OBJECT) {
				if (!object) {
					statementOrders.add(StatementOrder.O);
					break;
				}
			}
		}
		return statementOrders;
	}

	@Override
	public Set<StatementOrder> getSupportedOrders(Resource subj, IRI pred, Value obj, Resource... contexts) {

		if (!enableMergeJoin) {
			return Set.of();
		}

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
		long subjectID = this.endpoint.getHdtConverter().subjectToID(subj);
		long predicateID = this.endpoint.getHdtConverter().predicateToID(pred);
		long objectID = this.endpoint.getHdtConverter().objectToID(obj);

		if (subjectID == 0 || subjectID == -1) {
			newSubj = subj;
		} else {
			newSubj = this.endpoint.getHdtConverter().subjectIdToIRI(subjectID);
		}
		if (predicateID == 0 || predicateID == -1) {
			newPred = pred;
		} else {
			newPred = this.endpoint.getHdtConverter().predicateIdToIRI(predicateID);
		}
		if (objectID == 0 || objectID == -1) {
			newObj = obj;
		} else {
			newObj = this.endpoint.getHdtConverter().objectIdToIRI(objectID);
		}

		logger.debug("getSupportedOrders {} {} {}", newSubj, newPred, newObj);

		// check if we need to search over the delta, in which case the
		// statements can not be ordered
		if (shouldSearchOverNativeStore(subjectID, predicateID, objectID)) {
			return Set.of();
		}

		// iterate over the HDT file
		if (subjectID != -1 && predicateID != -1 && objectID != -1) {
			TripleID t = new TripleID(subjectID, predicateID, objectID);
			// search with the ID to check if the triples has been deleted
			List<TripleComponentOrder> tripleComponentOrder = this.endpoint.getHdt().getTriples()
					.getTripleComponentOrder(t);

			var orders = tripleComponentOrder.stream()
					.map(o -> getStatementOrder(o, subj != null, pred != null, obj != null)).flatMap(Collection::stream)
					.filter(p -> p != StatementOrder.P)
					// we do not support predicate ordering since it doesn't use
					// the same IDs as other IRIs
					.collect(Collectors.toSet());

			if (logger.isDebugEnabled()) {
				logger.debug("Triple pattern: {}\nMatching indexes: {}\nPossible orders: {}", t.getPatternString(),
						Arrays.toString(tripleComponentOrder.toArray()), Arrays.toString(orders.toArray()));
			}

			return orders;

		} else {// no need to search over hdt
			return Set.of(StatementOrder.S, StatementOrder.P, StatementOrder.O, StatementOrder.C);
		}

	}

	@Override
	public Comparator<Value> getComparator() {
		return (o1, o2) -> {
			if (o1 instanceof HDTValue && o2 instanceof HDTValue) {
				assert ((HDTValue) o1).getHDTPosition() != 2 : "o1 is in predicate position";
				assert ((HDTValue) o2).getHDTPosition() != 2 : "o2 is in predicate position";

				return Long.compare(((HDTValue) o1).getHDTId(), ((HDTValue) o2).getHDTId());
			}
			throw new UnsupportedOperationException(
					"Cannot compare values of type " + o1.getClass() + " and " + o2.getClass());
		};
	}
}
