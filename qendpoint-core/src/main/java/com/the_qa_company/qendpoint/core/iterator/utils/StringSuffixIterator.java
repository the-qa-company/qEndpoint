package com.the_qa_company.qendpoint.core.iterator.utils;

import com.the_qa_company.qendpoint.core.util.string.ByteString;

import java.util.Iterator;

/**
 * Append a suffix to all the nodes of an Iterator
 *
 * @author Antoine Willerval
 */
public class StringSuffixIterator extends FetcherIterator<ByteString> {
	public static Iterator<? extends CharSequence> of(Iterator<? extends CharSequence> it, ByteString suffix) {
		if (suffix.isEmpty()) {
			return it;
		}
		return new StringSuffixIterator(suffix, it);
	}

	private final ByteString suffix;
	private final Iterator<? extends CharSequence> it;

	private StringSuffixIterator(ByteString suffix, Iterator<? extends CharSequence> it) {
		this.suffix = suffix;
		this.it = it;
	}

	@Override
	protected ByteString getNext() {
		if (!it.hasNext()) {
			return null;
		}

		return ByteString.of(it.next()).copyAppend(suffix);
	}
}
