package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.store.exception.EndpointTimeoutException;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.IndexReportingIterator;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
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
	private Statement next;

	public EndpointStoreTripleIterator(EndpointStoreConnection connection, EndpointTripleSource endpointTripleSource,
			IteratorTripleID iter, CloseableIteration<? extends Statement> repositoryResult) {
		this.connection = Objects.requireNonNull(connection, "connection can't be null!");
		this.endpoint = Objects.requireNonNull(connection.getEndpoint(), "endpoint can't be null!");
		this.endpointTripleSource = Objects.requireNonNull(endpointTripleSource, "endpointTripleSource can't be null!");
		this.iterator = Objects.requireNonNull(iter, "iter can't be null!");
		this.repositoryResult = Objects.requireNonNull(repositoryResult, "repositoryResult can't be null!");
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
			long index = iterator.getLastTriplePosition();
			TripleComponentOrder order = iterator.isLastTriplePositionBoundToOrder() ? iterator.getOrder()
					: TripleComponentOrder.SPO;
			if (!endpoint.getDeleteBitMap(order).access(tripleID.isQuad() ? tripleID.getGraph() - 1 : 0, index)) {
				Resource subject = endpoint.getHdtConverter().idToSubjectHDTResource(tripleID.getSubject());
				IRI predicate = endpoint.getHdtConverter().idToPredicateHDTResource(tripleID.getPredicate());
				Value object = endpoint.getHdtConverter().idToObjectHDTResource(tripleID.getObject());
				if (logger.isTraceEnabled()) {
					logger.trace("From HDT   {} {} {} ", subject, predicate, object);
				}
				if (supportGraphs) {
					Resource ctx = tripleID.isQuad()
							? endpoint.getHdtConverter().idToGraphHDTResource(tripleID.getGraph())
							: null;
					next = endpointTripleSource.getValueFactory().createStatement(subject, predicate, object, ctx);
				} else {
					next = endpointTripleSource.getValueFactory().createStatement(subject, predicate, object);
				}
				return true;
			}
		}
		// iterate over the result of rdf4j
		if (this.repositoryResult.hasNext()) {
			Statement stm = repositoryResult.next();
			Resource newSubj = endpoint.getHdtConverter().rdf4jToHdtIDsubject(stm.getSubject());
			IRI newPred = endpoint.getHdtConverter().rdf4jToHdtIDpredicate(stm.getPredicate());
			Value newObject = endpoint.getHdtConverter().rdf4jToHdtIDobject(stm.getObject());
			Resource newContext = endpoint.getHdtConverter().rdf4jToHdtIDcontext(stm.getContext());

			next = endpointTripleSource.getValueFactory().createStatement(newSubj, newPred, newObject, newContext);
			if (logger.isTraceEnabled()) {
				logger.trace("From RDF4j {} {} {}", next.getSubject(), next.getPredicate(), next.getObject());
			}
			return true;
		}
		return false;
	}

	@Override
	public Statement next() {
		if (!hasNext()) {
			return null;
		}
		Statement stm = endpointTripleSource.getValueFactory().createStatement(next.getSubject(), next.getPredicate(),
				next.getObject(), next.getContext());
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
