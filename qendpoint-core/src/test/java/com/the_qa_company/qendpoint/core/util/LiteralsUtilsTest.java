package com.the_qa_company.qendpoint.core.util;

import org.junit.Test;
import com.the_qa_company.qendpoint.core.util.string.CharSequenceComparator;
import com.the_qa_company.qendpoint.core.util.string.CompactString;

import java.util.function.Supplier;

import static com.the_qa_company.qendpoint.core.util.LiteralsUtils.DATATYPE_HIGH_BYTE_BS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LiteralsUtilsTest {
	/**
	 * convert to compact string if required
	 *
	 * @param excepted excepted
	 * @param actual   actual
	 */
	public static void assertEqualsCompact(CharSequence excepted, CharSequence actual) {
		if (excepted instanceof String && actual instanceof String) {
			assertEquals(excepted, actual);
			return;
		}

		if (excepted instanceof String) {
			assertEquals(new CompactString(excepted), actual);
			return;
		}
		if (actual instanceof String) {
			assertEquals(excepted, new CompactString(actual));
			return;
		}

		assertEquals(0, CharSequenceComparator.getInstance().compare(excepted, actual));
	}

	private static final String LIT_TYPE_DEL = new String(new byte[] { LiteralsUtils.DATATYPE_BYTE });

	@Test
	public void containsLanguageTest() {
		assertTrue(LiteralsUtils.containsLanguage("\"hello\"@fr"));
		assertTrue(LiteralsUtils.containsLanguage("\"hello\"@fr-ca"));
		assertFalse(LiteralsUtils.containsLanguage("\"hello\"^^<http://test@example.org>"));
		assertFalse(LiteralsUtils.containsLanguage("\"hello\""));
		assertFalse(LiteralsUtils.containsLanguage("<http://test@example.org>"));
	}

	@Test
	public void removeTypeTest() {
		assertEqualsCompact("\"hello\"@fr", LiteralsUtils.removeType("\"hello\"@fr"));
		assertEqualsCompact("\"hello\"@fr-ca", LiteralsUtils.removeType("\"hello\"@fr-ca"));
		assertEqualsCompact("\"hello\"", LiteralsUtils.removeType("\"hello\"^^<http://test@example.org>"));
		assertEqualsCompact("\"hello\"", LiteralsUtils.removeType("\"hello\""));
		assertEqualsCompact("<http://test@example.org>", LiteralsUtils.removeType("<http://test@example.org>"));
	}

	@Test
	public void removeLangTest() {
		assertEqualsCompact("\"hello\"", LiteralsUtils.removeLang("\"hello\"@fr"));
		assertEqualsCompact("\"hello\"", LiteralsUtils.removeLang("\"hello\"@fr-ca"));
		assertEqualsCompact("\"hello\"^^<http://test@example.org>",
				LiteralsUtils.removeLang("\"hello\"^^<http://test@example.org>"));
		assertEqualsCompact("\"hello\"", LiteralsUtils.removeLang("\"hello\""));
		assertEqualsCompact("<http://test@example.org>", LiteralsUtils.removeLang("<http://test@example.org>"));
	}

	@Test
	public void getTypeTest() {
		assertEqualsCompact(LiteralsUtils.LITERAL_LANG_TYPE, LiteralsUtils.getType("\"hello\"@fr"));
		assertEqualsCompact(LiteralsUtils.LITERAL_LANG_TYPE, LiteralsUtils.getType("\"hello\"@fr-ca"));
		assertEqualsCompact("<http://test@example.org>", LiteralsUtils.getType("\"hello\"^^<http://test@example.org>"));
		assertEqualsCompact("<http://test@example.org#^^>",
				LiteralsUtils.getType("\"hello\"^^<http://test@example.org#^^>"));
		assertEqualsCompact(LiteralsUtils.NO_DATATYPE, LiteralsUtils.getType("\"hello\""));
		assertEqualsCompact(LiteralsUtils.NO_DATATYPE, LiteralsUtils.getType("<http://test@example.org>"));
	}

	@Test
	public void litStrTest() {
		assertTrue(LiteralsUtils.isLangType(LiteralsUtils.LITERAL_LANG_TYPE, 0));
		assertTrue(LiteralsUtils.isLangType(LiteralsUtils.LITERAL_LANG_TYPE_STR, 0));
	}

	@Test
	public void litToPrefTest() {
		assertEqualsCompact("\"aaa\"", LiteralsUtils.litToPref("\"aaa\""));
		assertEqualsCompact(LIT_TYPE_DEL + "<http://p>\"aaa\"", LiteralsUtils.litToPref("\"aaa\"^^<http://p>"));
		assertEqualsCompact(LIT_TYPE_DEL + LiteralsUtils.LITERAL_LANG_TYPE_STR + "\"aaa\"@fr-fr",
				LiteralsUtils.litToPref("\"aaa\"@fr-fr"));

		assertEqualsCompact("\"aaa\"", LiteralsUtils.litToPref(LiteralsUtils.prefToLit("\"aaa\"")));
		assertEqualsCompact(LIT_TYPE_DEL + "<http://p>\"aaa\"",
				LiteralsUtils.litToPref(LiteralsUtils.prefToLit(LIT_TYPE_DEL + "<http://p>\"aaa\"")));
		assertEqualsCompact(LIT_TYPE_DEL + LiteralsUtils.LITERAL_LANG_TYPE_STR + "\"aaa\"@fr-fr",
				LiteralsUtils.litToPref(
						LiteralsUtils.prefToLit(LIT_TYPE_DEL + LiteralsUtils.LITERAL_LANG_TYPE_STR + "\"aaa\"@fr-fr")));

		assertEqualsCompact("<http://test@example.org>", LiteralsUtils.litToPref("<http://test@example.org>"));
	}

	@Test
	public void litToPrefLangTest() {
		assertEqualsCompact("\"aaa\"", LiteralsUtils.litToPrefLang("\"aaa\""));
		assertEqualsCompact(DATATYPE_HIGH_BYTE_BS + "<http://p>\"aaa\"",
				LiteralsUtils.litToPrefLang("\"aaa\"^^<http://p>"));
		assertEqualsCompact(DATATYPE_HIGH_BYTE_BS + "@fr-fr\"aaa\"", LiteralsUtils.litToPrefLang("\"aaa\"@fr-fr"));

		assertEqualsCompact("\"aaa\"", LiteralsUtils.litToPrefLang(LiteralsUtils.prefToLitLang("\"aaa\"")));
		assertEqualsCompact("\"aaa\"^^<http://p>",
				LiteralsUtils.prefToLitLang(DATATYPE_HIGH_BYTE_BS + "<http://p>\"aaa\""));
		assertEqualsCompact(DATATYPE_HIGH_BYTE_BS + "<http://p>\"aaa\"",
				LiteralsUtils.litToPrefLang(LiteralsUtils.prefToLitLang(DATATYPE_HIGH_BYTE_BS + "<http://p>\"aaa\"")));
		assertEqualsCompact("\"aaa\"@fr-fr", LiteralsUtils.prefToLitLang(DATATYPE_HIGH_BYTE_BS + "@fr-fr\"aaa\""));
		assertEqualsCompact(DATATYPE_HIGH_BYTE_BS + "@fr-fr\"aaa\"",
				LiteralsUtils.litToPrefLang(LiteralsUtils.prefToLitLang(DATATYPE_HIGH_BYTE_BS + "@fr-fr\"aaa\"")));

		assertEqualsCompact("<http://test@example.org>", LiteralsUtils.litToPrefLang("<http://test@example.org>"));
		assertEqualsCompact("_:qzdzd", LiteralsUtils.litToPrefLang("_:qzdzd"));

		assertEqualsCompact("<http://test@example.org>", LiteralsUtils.prefToLitLang("<http://test@example.org>"));
		assertEqualsCompact("_:qzdzd", LiteralsUtils.prefToLitLang("_:qzdzd"));
	}

	@Test
	public void prefToLitTest() {
		assertEqualsCompact("\"aaa\"", LiteralsUtils.litToPref("\"aaa\""));
		assertEqualsCompact("\"aaa\"^^<http://p>", LiteralsUtils.prefToLit(LIT_TYPE_DEL + "<http://p>\"aaa\""));
		assertEqualsCompact("\"aaa\"@fr-fr",
				LiteralsUtils.prefToLit(LIT_TYPE_DEL + LiteralsUtils.LITERAL_LANG_TYPE_STR + "\"aaa\"@fr-fr"));
		assertEqualsCompact("<http://test@example.org>", LiteralsUtils.prefToLit("<http://test@example.org>"));

		assertEqualsCompact("\"aaa\"", LiteralsUtils.prefToLit(LiteralsUtils.litToPref("\"aaa\"")));
		assertEqualsCompact("\"aaa\"^^<http://p>",
				LiteralsUtils.prefToLit(LiteralsUtils.litToPref("\"aaa\"^^<http://p>")));
		assertEqualsCompact("\"aaa\"@fr-fr", LiteralsUtils.prefToLit(LiteralsUtils.litToPref("\"aaa\"@fr-fr")));
		assertEqualsCompact("<http://test@example.org>",
				LiteralsUtils.prefToLit(LiteralsUtils.litToPref("<http://test@example.org>")));
	}

	@Test
	public void removePrefTypeTest() {
		assertEqualsCompact("\"hello\"@fr", LiteralsUtils.removePrefType("\"hello\"@fr"));
		assertEqualsCompact("\"hello\"@fr-ca", LiteralsUtils.removePrefType("\"hello\"@fr-ca"));
		assertEqualsCompact("\"hello\"", LiteralsUtils.removePrefType("^^<http://test@example.org>\"hello\""));
		assertEqualsCompact("\"hello\"", LiteralsUtils.removePrefType("\"hello\""));
		assertEqualsCompact("<http://test@example.org>", LiteralsUtils.removePrefType("<http://test@example.org>"));
	}

	@Test
	public void getLanguageTest() {
		Supplier<AssertionError> cantFind = () -> new AssertionError("Can't find language");

		assertEqualsCompact("fr-fr", LiteralsUtils.getLanguage("\"test\"@fr-fr").orElseThrow(cantFind));
		assertEqualsCompact("en", LiteralsUtils.getLanguage("\"test\"@en").orElseThrow(cantFind));
	}
}
