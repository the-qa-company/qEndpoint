package com.the_qa_company.qendpoint.core.util;

import com.the_qa_company.qendpoint.core.util.string.ByteString;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

public class CustomIterator implements Iterator<CharSequence> {
	public ByteString prev = ByteString.empty();
	boolean first = true;
	Iterator<? extends CharSequence> iter;
	Map<? extends CharSequence, Long> literalsCounts;
	private long currCount;

	private final boolean lang;

	public CustomIterator(Iterator<? extends CharSequence> iter, Map<? extends CharSequence, Long> literalsCounts,
			boolean lang) {
		this.iter = iter;
		this.literalsCounts = literalsCounts;
		this.lang = lang;
		if (iter.hasNext()) {
			prev = ByteString.of(iter.next());

			if (lang) {
				CharSequence type = LiteralsUtils.getType(prev);
				if (LiteralsUtils.LITERAL_LANG_TYPE.equals(type)) {
					Optional<CharSequence> lg = LiteralsUtils.getLanguage(prev);
					currCount = literalsCounts.get(ByteString.of("@").copyAppend(lg.orElseThrow()));
				} else {
					currCount = literalsCounts.get(type);
				}
			} else {
				currCount = literalsCounts.get(LiteralsUtils.getType(prev));
			}
			currCount--;
		} else {
			first = false;
		}
	}

	@Override
	public boolean hasNext() {
		if (currCount == 0) {
			if (first)
				return true;
			if (iter.hasNext()) {
				prev = ByteString.of(iter.next());
				if (lang) {
					CharSequence type = LiteralsUtils.getType(prev);
					if (LiteralsUtils.LITERAL_LANG_TYPE.equals(type)) {
						Optional<CharSequence> lg = LiteralsUtils.getLanguage(prev);
						currCount = literalsCounts.get(ByteString.of("@").copyAppend(lg.orElseThrow()));
					} else {
						currCount = literalsCounts.get(type);
					}
				} else {
					currCount = literalsCounts.get(LiteralsUtils.getType(prev));
				}
				currCount--;
				first = true;
			}
			return false;
		} else {
			return true;
		}
	}

	@Override
	public CharSequence next() {
		if (first) {
			first = false;
		} else {
			prev = ByteString.of(iter.next());
			currCount--;
		}
		if (lang) {
			return LiteralsUtils.removeTypeAndLang(prev);
		}
		return LiteralsUtils.removeType(prev);
	}
}
