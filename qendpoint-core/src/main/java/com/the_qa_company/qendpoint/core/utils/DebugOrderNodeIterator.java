package com.the_qa_company.qendpoint.core.utils;

import com.the_qa_company.qendpoint.core.triples.IndexedNode;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.CharSequenceComparator;
import com.the_qa_company.qendpoint.core.util.string.ReplazableString;

import java.util.Comparator;
import java.util.function.Consumer;

public class DebugOrderNodeIterator implements Consumer<IndexedNode> {
	private static boolean assertEnabled;
	static {
		try {
			assert false;
		} catch (AssertionError e) {
			assertEnabled = true;
		}
	}

	public static boolean isAssertEnable() {
		return assertEnabled;
	}

	public static Consumer<IndexedNode> of(String name) {
		return of(name, false);
	}

	public static Consumer<IndexedNode> of(String name, boolean allowDuplicated) {
		return of(isAssertEnable(), name, allowDuplicated);
	}

	public static Consumer<IndexedNode> of(boolean debug, String name) {
		return of(debug, name, false);
	}

	public static Consumer<IndexedNode> of(boolean debug, String name, boolean allowDuplicated) {
		return of(debug, name, allowDuplicated, CharSequenceComparator.getInstance());
	}

	public static Consumer<IndexedNode> of(boolean debug, String name, boolean allowDuplicated,
			Comparator<CharSequence> comparator) {
		if (debug) {
			return new DebugOrderNodeIterator(comparator, name, allowDuplicated);
		}
		return (t) -> {};
	}

	private final Comparator<CharSequence> comparator;
	private final String name;
	private final ReplazableString prevBuffer = new ReplazableString(16);
	private final boolean allowDuplicated;

	private DebugOrderNodeIterator(Comparator<CharSequence> comparator, String name, boolean allowDuplicated) {
		this.comparator = comparator;
		this.name = name;
		this.allowDuplicated = allowDuplicated;
	}

	@Override
	public void accept(IndexedNode obj) {
		ByteString node = obj.getNode();
		if (prevBuffer.length() != 0) {
			int cmp = comparator.compare(prevBuffer, node);
			if (cmp == 0 && !allowDuplicated) {
				throw new AssertionError("DUPLICATION ERROR: prevBuffer == comparator for string '" + node + "' == '"
						+ prevBuffer + "' in section " + name);
			}
			if (cmp > 0) {
				throw new AssertionError("ORDER ERROR: prevBuffer > comparator for string '" + node + "' > '"
						+ prevBuffer + "' in section " + name);
			}
		}
		prevBuffer.replace(node);
	}

}
