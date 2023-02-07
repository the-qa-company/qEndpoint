package com.the_qa_company.qendpoint.core.dictionary.impl;

import com.the_qa_company.qendpoint.core.dictionary.TempDictionary;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.util.string.CompactString;
import com.the_qa_company.qendpoint.core.util.string.DelayedString;
import com.the_qa_company.qendpoint.core.util.string.ReplazableString;

/**
 * Prefix AND Suffix front-coded dictionary encodes strings so
 * PFCDictionarySection will encode more efficiently than w/the standard NTriple
 * uri/literal/blank node format. Specifically, it moves the lang and datatype
 * suffix for literals to the front of the string.
 */
public class PSFCFourSectionDictionary extends FourSectionDictionary {

	public PSFCFourSectionDictionary(HDTOptions spec) {
		super(spec);
	}

	static CharSequence encode(CharSequence str) {
		// Example:
		// "123"^^<http://www.w3.org/2001/XMLSchema#integer> is encoded to
		// ^^<http://www.w3.org/2001/XMLSchema#integer>"123"
		CharSequence s = DelayedString.unwrap(str);
		if (s == null || s.length() == 0 || s.charAt(0) != '"' || s.charAt(s.length() - 1) == '"') {
			return str;
		} else if (s instanceof CompactString) {
			// Avoid UTF-8 encoder round trip
			CompactString cs = (CompactString) s;
			return swap(cs, cs.lastIndexOf('"') + 1);
		} else {
			String string = s.toString();
			return swap(string, string.lastIndexOf('"') + 1);
		}
	}

	static CharSequence decode(CharSequence str) {
		// Example:
		// ^^<http://www.w3.org/2001/XMLSchema#integer>"123" is decoded to
		// "123"^^<http://www.w3.org/2001/XMLSchema#integer>
		CharSequence s = DelayedString.unwrap(str);
		char ch;
		if (s == null || s.length() == 0 || ((ch = s.charAt(0)) != '^' && ch != '@')
				|| s.charAt(s.length() - 1) != '"') {
			return str;
		} else if (s instanceof CompactString) {
			// Avoid UTF-8 encoder round trip
			CompactString cs = (CompactString) s;
			return swap(cs, cs.indexOf('"'));
		} else {
			String string = s.toString();
			return swap(string, string.indexOf('"'));
		}
	}

	@Override
	public long stringToId(CharSequence str, TripleComponentRole position) {
		return super.stringToId(encode(str), position);
	}

	@Override
	public CharSequence idToString(long id, TripleComponentRole position) {
		return decode(super.idToString(id, position));
	}

	@Override
	public String getType() {
		return HDTVocabulary.DICTIONARY_TYPE_FOUR_PSFC_SECTION;
	}

	@Override
	public void load(TempDictionary other, ProgressListener listener) {
		if (!(other instanceof PSFCTempDictionary)) {
			throw new IllegalArgumentException("Expected " + PSFCTempDictionary.class.getName());
		}
		super.load(other, listener);
	}

	/**
	 * Given a compact string "[prefix][suffix]" where 'prefix' has length
	 * 'offset' returns "[suffix][prefix]".
	 */
	private static CharSequence swap(CompactString cs, int offset) {
		ReplazableString buf = new ReplazableString(cs.length());
		buf.append(cs.getData(), offset, cs.length() - offset);
		buf.append(cs.getData(), 0, offset);
		return new CompactString(buf).getDelayed();
	}

	/**
	 * Given a string string "[prefix][suffix]" where 'prefix' has length
	 * 'offset' returns "[suffix][prefix]".
	 */
	private static String swap(String string, int offset) {
		StringBuilder buf = new StringBuilder(string.length());
		buf.append(string, offset, string.length());
		buf.append(string, 0, offset);
		return buf.toString();
	}
}
