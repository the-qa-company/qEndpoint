package com.the_qa_company.qendpoint.core.iterator.utils;

import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.CompactString;

import java.util.Iterator;

public class StringQuotesSuffixIterator extends FetcherIterator<ByteString> {
	public static Iterator<? extends CharSequence> of(Iterator<? extends CharSequence> it, ByteString suffix) {
		if (suffix.isEmpty()) {
			return it;
		}
		return new StringQuotesSuffixIterator(suffix, it);
	}

	private final ByteString suffix;
	private final Iterator<? extends CharSequence> it;

	private StringQuotesSuffixIterator(ByteString suffix, Iterator<? extends CharSequence> it) {
		this.suffix = suffix;
		this.it = it;
	}

	@Override
	protected ByteString getNext() {
		if (!it.hasNext()) {
			return null;
		}

		ByteString obs = ByteString.of(it.next());
		byte[] buffer = new byte[suffix.length() + obs.length() + 2];

		buffer[0] = '"';
		System.arraycopy(obs.getBuffer(), 0, buffer, 1, obs.length());
		buffer[obs.length() + 1] = '"';
		System.arraycopy(suffix.getBuffer(), 0, buffer, obs.length() + 2, suffix.length());

		return new CompactString(buffer);
	}
}
