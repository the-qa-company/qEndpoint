package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.FetcherExceptionIterator;
import com.the_qa_company.qendpoint.core.triples.TripleID;

public class DebugCheckTripleIDOrderterator<E extends Exception> extends FetcherExceptionIterator<TripleID, E> {
	private final ExceptionIterator<TripleID, E> it;
	private final TripleID[] lasts = new TripleID[10];
	private long lastIdx;
	private final TripleID last = new TripleID();

	public DebugCheckTripleIDOrderterator(ExceptionIterator<TripleID, E> it) {
		this.it = it;
		for (int i = 0; i < lasts.length; i++) {
			lasts[i] = new TripleID();
		}
	}

	private String getLasts() {
		StringBuilder b = new StringBuilder("lasts: " + lastIdx);

		for (long i = Math.max(0, lastIdx - lasts.length); i < lastIdx; i++) {
			b.append("\n").append(lasts[(int)(i % lasts.length)].toString());
		}

		return b.toString();
	}

	private void setLast(TripleID next) {
		last.assign(next);
		lasts[(int)(lastIdx % lasts.length)].assign(next);
		lastIdx++;
	}

	@Override
	protected TripleID getNext() throws E {
		if (!it.hasNext()) {
			return null;
		}
		TripleID next = it.next();

		int c = next.compareTo(last);
		if (c == 0) {
			throw new RuntimeException("Invalid subject order: " + next + " == " + last + " (duplicated) " + getLasts());
		}
		if (c < 0) {
			throw new RuntimeException("Invalid subject order: " + next + " <= " + last + " (ordered) " + getLasts());
		}

		setLast(next);

		return next;
	}
}
