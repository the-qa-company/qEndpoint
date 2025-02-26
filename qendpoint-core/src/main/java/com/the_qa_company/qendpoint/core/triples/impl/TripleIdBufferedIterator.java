package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.FetcherExceptionIterator;
import com.the_qa_company.qendpoint.core.triples.TripleID;

import java.io.IOException;

public class TripleIdBufferedIterator<E extends Exception> extends FetcherExceptionIterator<TripleID, E> {
	public static ExceptionIterator<TripleID, IOException> of(ExceptionIterator<TripleID, IOException> it, int bufferSize) {
		if (bufferSize < 1) throw new IllegalArgumentException("Invalid buffer size: " + bufferSize);
		if (bufferSize == 1) return it;
		return new TripleIdBufferedIterator<>(it, bufferSize);
	}
	private final ExceptionIterator<TripleID, E> origin;

	private final TripleID[] buffer;
	private int bufferPointer;
	private int bufferSize;

	public TripleIdBufferedIterator(ExceptionIterator<TripleID, E> origin, int bufferSize) {
		this.origin = origin;
		buffer = new TripleID[bufferSize];
	}


	@Override
	protected TripleID getNext() throws E {
		if (bufferPointer < bufferSize) {
			return buffer[bufferPointer++];
		}

		if (!origin.hasNext()) {
			return null; // no data remaining
		}

		bufferSize = 0;

		do {
			if (buffer[bufferSize] != null) {
				buffer[bufferSize++].assign(origin.next());
			} else {
				buffer[bufferSize++] = origin.next().clone();
			}
		} while (origin.hasNext() && bufferSize < buffer.length);

		bufferPointer = 1; // we know we read at least one elem
		return buffer[0];
	}
}
