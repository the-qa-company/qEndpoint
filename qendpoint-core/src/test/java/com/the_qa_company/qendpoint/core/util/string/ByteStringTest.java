package com.the_qa_company.qendpoint.core.util.string;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ByteStringTest {
	@Test
	@Ignore("failing https://github.com/rdfhdt/hdt-java/issues/177")
	public void utf32Test() {
		String ss1 = new String(Character.toChars(0x26ce3)); // 𦳣
		String ss2 = new String(Character.toChars(0xf4d1)); // 

		System.out.println(ss1.compareTo(ss2));
		System.out.println(Integer.compare(0x26ce3, 0xf4d1));

		CompactString b1 = new CompactString(ss1);
		CompactString b2 = new CompactString(ss2);

		assertEquals(ss1, b1.toString());
		assertEquals(ss2, b2.toString());

		int cmpByte = Math.max(-1, Math.min(1, b1.compareTo(b2)));
		int cmpStr = Math.max(-1, Math.min(1, b1.toString().compareTo(b2.toString())));

		assertEquals(cmpStr, cmpByte);
	}
}
