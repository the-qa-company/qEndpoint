package com.the_qa_company.qendpoint.core.triples;

import org.junit.Assert;
import org.junit.Test;

public class TripleStringTest {
	private static class CustomCharSequence implements CharSequence {

		private final String str;

		CustomCharSequence(String str) {
			this.str = str;
		}

		@Override
		public int length() {
			return str.length();
		}

		@Override
		public char charAt(int index) {
			return str.charAt(index);
		}

		@Override
		public CharSequence subSequence(int start, int end) {
			return new CustomCharSequence(str.substring(start, end));
		}
	}

	private TripleString customTripleStr(String s, String p, String o) {
		return new TripleString(new CustomCharSequence(s), new CustomCharSequence(p), new CustomCharSequence(o));
	}

	@Test
	public void tripleStringEqualsTest() {
		String baseUri = "http://example.org/";

		String s = baseUri + "subject";
		String p = baseUri + "predicate";
		String o = baseUri + "object";
		String s2 = baseUri + "subject2";
		String p2 = baseUri + "predicate2";
		String o2 = baseUri + "object2";

		TripleString ts1 = customTripleStr(s, p, o);
		TripleString ts2 = customTripleStr(s, p, o);
		TripleString ts3 = customTripleStr(s2, p2, o2);

		Assert.assertEquals("ts1=ts1", ts1, ts1);
		Assert.assertEquals("ts2=ts2", ts2, ts2);
		Assert.assertEquals("ts1=ts2", ts1, ts2);
		Assert.assertNotEquals("ts1!=ts3", ts1, ts3);
	}

	@Test
	public void tripleStringMatchTest() {
		String baseUri = "http://example.org/";

		String s = baseUri + "subject";
		String p = baseUri + "predicate";
		String o = baseUri + "object";
		String s2 = baseUri + "subject2";
		String p2 = baseUri + "predicate2";
		String o2 = baseUri + "object2";

		TripleString ts1 = customTripleStr(s, p, o);
		Assert.assertTrue("ts1 match itself", ts1.match(ts1));
		Assert.assertTrue("ts1 match itself", ts1.match(customTripleStr(s, p, o)));
		Assert.assertTrue("ts1 match sp?", ts1.match(customTripleStr(s, p, "")));
		Assert.assertTrue("ts1 match ?p?", ts1.match(customTripleStr("", p, "")));
		Assert.assertTrue("ts1 match s??", ts1.match(customTripleStr(s, "", "")));
		Assert.assertTrue("ts1 match ???", ts1.match(customTripleStr("", "", "")));

		Assert.assertFalse("ts1 not match itself", ts1.match(customTripleStr(s2, p2, o)));
		Assert.assertFalse("ts1 not match 2 sp?", ts1.match(customTripleStr(s2, p2, "")));
		Assert.assertFalse("ts1 not match 2 ?p?", ts1.match(customTripleStr("", p2, "")));
		Assert.assertFalse("ts1 not match 2 s??", ts1.match(customTripleStr(s2, "", "")));
	}

}
