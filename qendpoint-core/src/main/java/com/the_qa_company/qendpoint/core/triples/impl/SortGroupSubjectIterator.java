package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.FetcherExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.PeekExceptionIterator;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.util.io.Closer;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class SortGroupSubjectIterator<E extends Exception> extends FetcherExceptionIterator<TripleID, E>
		implements Closeable {
	public static <E extends Exception> PeekExceptionIterator<TripleID, E> of(ExceptionIterator<TripleID, E> it) {
		return new SortGroupSubjectIterator<>(it);
	}

	private static final Comparator<TripleID> PO_COMPARATOR = Comparator.comparingLong(TripleID::getPredicate)
			.thenComparingLong(TripleID::getObject);

	private final PeekExceptionIterator<TripleID, E> it;
	private Iterator<TripleID> nextBlock;

	private SortGroupSubjectIterator(ExceptionIterator<TripleID, E> it) {
		this.it = PeekExceptionIterator.of(it);
	}

	@Override
	protected TripleID getNext() throws E {
		if (nextBlock != null) {
			if (nextBlock.hasNext()) {
				return nextBlock.next();
			}
			nextBlock = null; // cleanup
		}

		if (!it.hasNext()) {
			return null; // nothing
		}

		TripleID first = it.next();

		long currentSubject = first.getSubject();

		if (!(it.hasNext() && it.peek().getSubject() == currentSubject)) {
			return first; // only one subject, no need to sort
		}

		// fetch and sort the group
		List<TripleID> group = new ArrayList<>();
		group.add(first);

		do {
			group.add(it.next());
		} while (it.hasNext() && it.peek().getSubject() == currentSubject);

		group.sort(PO_COMPARATOR);

		nextBlock = group.iterator();
		return nextBlock.next();
	}

	@Override
	public void close() throws IOException {
		Closer.closeSingle(it);
	}
}
