package com.the_qa_company.qendpoint.core.util.string;

import com.the_qa_company.qendpoint.core.util.LiteralsUtils;

import java.math.BigDecimal;
import java.util.Comparator;

public class CharSequenceRawComparator implements Comparator<CharSequence> {

	private static final Comparator<CharSequence> instance = new CharSequenceRawComparator();

	public static ByteString getDTLType(ByteString s) {
		ByteString type = (ByteString)LiteralsUtils.getType(s);
		if (LiteralsUtils.LITERAL_LANG_TYPE == type) {
			return LiteralsUtils.LANG_OPERATOR.copyAppend(LiteralsUtils.getLanguage(s).orElseThrow());
		}
		if (LiteralsUtils.NO_DATATYPE == type) {
			return CharSequenceDTLComparator.DTL_DTN;
		}
		return type;
	}

	public static Comparator<CharSequence> getInstance() {
		return instance;
	}

	private final Comparator<CharSequence> base = CharSequenceComparator.getInstance();

	/*
	 * (non-Javadoc)
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	@Override
	public int compare(CharSequence s1, CharSequence s2) {
		if (s1 == s2) {
			return 0;
		}
		ByteString bs1 = ByteString.of(s1);
		ByteString bs2 = ByteString.of(s2);

		ByteString rt1 = getDTLType(bs1);
		ByteString rt2 = getDTLType(bs2);

		int x = base.compare(rt1, rt2);

		if (x != 0) {
			return x;
		}

		if (rt1 == CharSequenceDTLComparator.DTL_DTN || rt1.charAt(0) == '@') {
			// lang or ndt
			return base.compare(s1, s2);
		}

		ByteString kdt1 = RawStringUtils.rawKnownDataType(rt1);

		// check number values
		if (kdt1 == RawStringUtils.XSD_DECIMAL_DT) {
			CharSequence n1 = LiteralsUtils.removeQuotesTypeAndLang(bs1);
			CharSequence n2 = LiteralsUtils.removeQuotesTypeAndLang(bs2);

			return new BigDecimal(n1.toString()).compareTo(new BigDecimal(n2.toString()));
		}

		if (kdt1 == RawStringUtils.XSD_DOUBLE_DT) {
			CharSequence n1 = LiteralsUtils.removeQuotesTypeAndLang(bs1);
			CharSequence n2 = LiteralsUtils.removeQuotesTypeAndLang(bs2);

			return Double.compare(Double.parseDouble(n1.toString()), Double.parseDouble(n2.toString()));
		}

		if (kdt1 == RawStringUtils.XSD_INTEGER_DT) {
			CharSequence n1 = LiteralsUtils.removeQuotesTypeAndLang(bs1);
			CharSequence n2 = LiteralsUtils.removeQuotesTypeAndLang(bs2);

			return Long.compare(Long.parseLong(n1, 0, n1.length(), 10), Long.parseLong(n2, 0, n2.length(), 10));
		}

		// unknown, use string value
		return base.compare(s1, s2);
	}

}
