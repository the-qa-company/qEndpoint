package com.the_qa_company.qendpoint.core.util;

import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;
import com.the_qa_company.qendpoint.core.enums.RDFNodeType;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.CompactString;
import com.the_qa_company.qendpoint.core.util.string.DelayedString;

/**
 * Index to store the {@link RDFNodeType} of a {@link DictionarySection}
 */
public class SortedDictionarySectionIndex {
	static final ByteString START_BNODE = ByteString.of("_:");
	static final ByteString START_LITERAL = ByteString.of("\"");
	static final ByteString END_BNODE = new CompactString(new byte[] { '_', (byte) (':' + 1) });
	static final ByteString END_LITERAL = new CompactString(new byte[] { (byte) ('"' + 1) });
	private DictionarySection section;
	long bnodeStart;
	long bnodeEnd;
	long literalStart;
	long literalEnd;

	public SortedDictionarySectionIndex(DictionarySection section) {
		setSection(section);
	}

	public void setSection(DictionarySection section) {
		this.section = section;
		syncLocation();
	}

	private void syncLocation() {
		if (section.getNumberOfElements() == 0) {
			return;
		}

		bnodeStart = binarySearch(START_BNODE, 0);
		bnodeEnd = binarySearch(END_BNODE, bnodeStart);

		literalStart = binarySearch(START_LITERAL, 0);
		literalEnd = binarySearch(END_LITERAL, literalStart);
	}

	long binarySearch(ByteString startSymbol, long startIndex) {
		long start = startIndex;
		long end = section.getNumberOfElements();

		while (start < end) {
			long middle = (start + end) / 2;

			if (middle == 0) {
				return 1;
			}

			ByteString node = (ByteString) DelayedString.unwrap(section.extract(middle));

			if (node.compareTo(startSymbol) < 0) {
				start = middle + 1;
			} else {
				end = middle;
			}
		}
		return end;
	}

	public RDFNodeType getNodeType(long id) {
		if (id > section.getNumberOfElements()) {
			return null;
		}
		if (id >= bnodeStart && id < bnodeEnd) {
			return RDFNodeType.BLANK_NODE;
		}
		if (id >= literalStart && id < literalEnd) {
			return RDFNodeType.LITERAL;
		}

		return RDFNodeType.IRI;
	}
}
