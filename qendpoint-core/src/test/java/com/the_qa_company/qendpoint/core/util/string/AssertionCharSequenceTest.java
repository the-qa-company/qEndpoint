package com.the_qa_company.qendpoint.core.util.string;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class AssertionCharSequenceTest {
	@Test
	public void afterBoundTest() {
		AssertionCharSequence seq = new AssertionCharSequence("aaabbb", 3);

		seq.charAt(3);
		seq.charAt(4);
		seq.charAt(5);

		seq.subSequence(3, 6);
	}

	@Test(expected = AssertionError.class)
	public void beforeBoundTest() {

		AssertionCharSequence seq = new AssertionCharSequence("aaabbb", 3);

		seq.charAt(2);
	}

	@Test(expected = AssertionError.class)
	public void toStringTest() {

		AssertionCharSequence seq = new AssertionCharSequence("aaabbb", 3);

		assertNotNull(seq.toString());
	}

	@Test(expected = AssertionError.class)
	public void subTest() {

		AssertionCharSequence seq = new AssertionCharSequence("aaabbb", 3);

		seq.subSequence(0, 3);
	}

}
