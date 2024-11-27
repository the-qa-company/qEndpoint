package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.core.compact.bitmap.MultiLayerBitmapWrapper;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.store.exception.EndpointTimeoutException;
import com.the_qa_company.qendpoint.utils.BitArrayDisk;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.IndexReportingIterator;
import org.eclipse.rdf4j.query.QueryInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class PrototypeEndpointStoreTripleIterator implements CloseableIteration<TripleID>, IndexReportingIterator {
	private static final Logger logger = LoggerFactory.getLogger(PrototypeEndpointStoreTripleIterator.class);

	private final AtomicBoolean closed = new AtomicBoolean();
	private final EndpointStore endpoint;
	private final EndpointStoreConnection connection;
	private final EndpointTripleSource endpointTripleSource;
	private final IteratorTripleID iterator;
	private TripleID next;

	public PrototypeEndpointStoreTripleIterator(EndpointStoreConnection connection,
			EndpointTripleSource endpointTripleSource, IteratorTripleID iter) {
		this.connection = Objects.requireNonNull(connection, "connection can't be null!");
		this.endpoint = Objects.requireNonNull(connection.getEndpoint(), "endpoint can't be null!");
		this.endpointTripleSource = Objects.requireNonNull(endpointTripleSource, "endpointTripleSource can't be null!");
		this.iterator = Objects.requireNonNull(iter, "iter can't be null!");
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
				next = tripleID;
				return true;
			}
		}

		return false;
	}

	@Override
	public TripleID next() {
		if (!hasNext()) {
			return null;
		}
		var stm = next;
		next = null;
		return stm;
	}

	@Override
	public void remove() {
		throw new RuntimeException("not implemented");
	}

	@Override
	public void close() {
		// no-op
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
