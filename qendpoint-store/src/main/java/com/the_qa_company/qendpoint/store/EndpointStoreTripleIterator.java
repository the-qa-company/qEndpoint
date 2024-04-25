package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.core.compact.bitmap.MultiLayerBitmapWrapper;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.store.exception.EndpointTimeoutException;
import com.the_qa_company.qendpoint.utils.BitArrayDisk;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.IndexReportingIterator;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.GenericStatement;
import org.eclipse.rdf4j.query.QueryInterruptedException;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class EndpointStoreTripleIterator implements CloseableIteration<Statement>, IndexReportingIterator {
	private static final Logger logger = LoggerFactory.getLogger(EndpointStoreTripleIterator.class);

	private final AtomicBoolean closed = new AtomicBoolean();
	private final EndpointStore endpoint;
	private final EndpointStoreConnection connection;
	private final EndpointTripleSource endpointTripleSource;
	private final IteratorTripleID iterator;
	private final CloseableIteration<? extends Statement> repositoryResult;

	private final Resource subject;
	private final IRI predicate;
	private final Value object;

	private long objectID_cache;
	private Value objectCache;

	private long subjectID_cache;
	private Resource subjectCache;

	private long predicateID_cache;
	private IRI predicateCache;

	private Statement next;

	public EndpointStoreTripleIterator(EndpointStoreConnection connection, EndpointTripleSource endpointTripleSource,
			IteratorTripleID iter, CloseableIteration<? extends Statement> repositoryResult, long subjectID,
			long predicateID, long objectID, boolean graph, long[] graphID) {
		this.connection = Objects.requireNonNull(connection, "connection can't be null!");
		this.endpoint = Objects.requireNonNull(connection.getEndpoint(), "endpoint can't be null!");
		this.endpointTripleSource = Objects.requireNonNull(endpointTripleSource, "endpointTripleSource can't be null!");
		this.iterator = Objects.requireNonNull(iter, "iter can't be null!");
		this.repositoryResult = Objects.requireNonNull(repositoryResult, "repositoryResult can't be null!");

		if (subjectID > 0) {
			subject = endpoint.getHdtConverter().idToSubjectHDTResource(subjectID);
//			System.out.println("PRE_CALC: "+subject);
		} else {
			subject = null;
		}

		if (predicateID > 0) {
			predicate = endpoint.getHdtConverter().idToPredicateHDTResource(predicateID);
//			System.out.println("PRE_CALC: "+predicate);
		} else {
			predicate = null;
		}

		if (objectID > 0) {
			object = endpoint.getHdtConverter().idToObjectHDTResource(objectID);
//			System.out.println("PRE_CALC: "+object);
		} else {
			object = null;
		}

	}

	@Override
	public boolean hasNext() {
		if (next != null) {
			return true;
		}
		if (closed.get()) {
			throw new QueryInterruptedException("closed iterator");
		}
		if (connection.isTimeout()) {
			throw new EndpointTimeoutException();
		}
		boolean supportGraphs = endpoint.getHdt().getDictionary().supportGraphs();
		// iterate over the result of hdt
		while (iterator.hasNext()) {
			TripleID tripleID = iterator.next();
			TripleComponentOrder order = iterator.isLastTriplePositionBoundToOrder() ? iterator.getOrder()
					: TripleComponentOrder.SPO;
			MultiLayerBitmapWrapper.MultiLayerModBitmapWrapper dbm = endpoint.getDeleteBitMap(order);
			if (endpoint.isDeleteDisabled() || dbm.<BitArrayDisk>getHandle().getMaxNumBits() == 0
					|| !dbm.access(tripleID.isQuad() ? tripleID.getGraph() - 1 : 0, iterator.getLastTriplePosition())) {

//				if (logger.isTraceEnabled()) {
//					logger.trace("From HDT   {} {} {} ", subject, predicate, object);
//				}
				if (supportGraphs) {
					createStatementWithContext(tripleID);
				} else {
					createStatementWithoutContext(tripleID);
				}
				return true;
			}
		}
		// iterate over the result of rdf4j
		if (this.repositoryResult.hasNext()) {
			iterateOverResultsFromRDF4J();
			return true;
		}
		return false;
	}

	private void createStatementWithoutContext(TripleID tripleID) {
		Resource subject = getSubject(tripleID);

		IRI predicate = getPredicate(tripleID);

		Value object = getObject(tripleID);

		next = new GenericStatement<>(subject, predicate, object, null);

//		next = endpointTripleSource.getValueFactory().createStatement(subject, predicate, object);
	}

	private void createStatementWithContext(TripleID tripleID) {
		Resource subject = endpoint.getHdtConverter().idToSubjectHDTResource(tripleID.getSubject());
		IRI predicate = endpoint.getHdtConverter().idToPredicateHDTResource(tripleID.getPredicate());
		Value object = endpoint.getHdtConverter().idToObjectHDTResource(tripleID.getObject());
		Resource ctx = tripleID.isQuad() ? endpoint.getHdtConverter().idToGraphHDTResource(tripleID.getGraph()) : null;
		next = endpointTripleSource.getValueFactory().createStatement(subject, predicate, object, ctx);
	}

	private Resource getSubject(TripleID tripleID) {
		Resource subject;
		if (this.subject != null) {
			subject = this.subject;
//		} else if (tripleID.getSubject() == subjectID_cache) {
//			subject = subjectCache;
		} else {
			subject = endpoint.getHdtConverter().idToSubjectHDTResource(tripleID.getSubject());
//			this.subjectID_cache = tripleID.getSubject();
//			this.subjectCache = subject;
		}
		return subject;
	}

	private IRI getPredicate(TripleID tripleID) {
		IRI predicate;
		if (this.predicate != null) {
			predicate = this.predicate;
//		} else if (tripleID.getPredicate() == predicateID_cache) {
//			predicate = predicateCache;
		} else {
			predicate = endpoint.getHdtConverter().idToPredicateHDTResource(tripleID.getPredicate());
//			this.predicateID_cache = tripleID.getPredicate();
//			this.predicateCache = predicate;
		}
		return predicate;
	}

	private Value getObject(TripleID tripleID) {
		Value object;
		if (this.object != null) {
			object = this.object;
		} else if (tripleID.getObject() == objectID_cache) {
			object = objectCache;
		} else {
			object = endpoint.getHdtConverter().idToObjectHDTResource(tripleID.getObject());
			this.objectID_cache = tripleID.getObject();
			this.objectCache = object;
		}
		return object;
	}

	private void iterateOverResultsFromRDF4J() {
		Statement stm = repositoryResult.next();
		Resource newSubj = endpoint.getHdtConverter().rdf4jToHdtIDsubject(stm.getSubject());
		IRI newPred = endpoint.getHdtConverter().rdf4jToHdtIDpredicate(stm.getPredicate());
		Value newObject = endpoint.getHdtConverter().rdf4jToHdtIDobject(stm.getObject());
		Resource newContext = endpoint.getHdtConverter().rdf4jToHdtIDcontext(stm.getContext());

		next = endpointTripleSource.getValueFactory().createStatement(newSubj, newPred, newObject, newContext);
		if (logger.isTraceEnabled()) {
			logger.trace("From RDF4j {} {} {}", next.getSubject(), next.getPredicate(), next.getObject());
		}
	}

	@Override
	public Statement next() {
		if (!hasNext()) {
			return null;
		}
		Statement stm = next;
		next = null;
		return stm;
	}

	@Override
	public void remove() {
		throw new RuntimeException("not implemented");
	}

	@Override
	public void close() {
		if (!closed.get()) {
			try {
				closed.set(true);
			} finally {
				repositoryResult.close();
			}
		}
	}

	@Override
	public String getIndexName() {
		TripleComponentOrder order = iterator.getOrder();
		if (order != null) {
			return order.name();
		}
		return null;
	}
}
