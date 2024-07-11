package com.the_qa_company.qendpoint.model;

import org.junit.Assert;
import org.junit.Test;

public class SimpleLiteralHDTTest {
	@Test
	public void indexOfCharTest() {
		String object = "0123456789abcdefghijklmnopqrstuvwxyz";
		for (int i = 0; i < object.length(); i++) {
			for (int j = 0; j <= i; j++) {
				Assert.assertEquals(i, SimpleLiteralHDT.indexOf(object, object.charAt(i), j));
			}
			for (int j = i + 1; j < object.length(); j++) {
				Assert.assertEquals(-1, SimpleLiteralHDT.indexOf(object, object.charAt(i), j));
			}
		}
	}

	@Test
	public void indexOfCharSeqTest() {
		String object = "0123456789abcdefghijklmnopqrstuvwxyz";
		for (int i = 0; i < object.length() - 1; i++) {
			String s = object.charAt(i) + String.valueOf(object.charAt(i + 1));
			for (int j = 0; j <= i; j++) {
				Assert.assertEquals(i, SimpleLiteralHDT.indexOf(object, s, j));
			}
			for (int j = i + 1; j < object.length(); j++) {
				Assert.assertEquals(-1, SimpleLiteralHDT.indexOf(object, s, j));
			}
		}
	}
}
