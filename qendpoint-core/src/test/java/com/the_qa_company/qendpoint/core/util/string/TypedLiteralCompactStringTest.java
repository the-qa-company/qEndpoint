package com.the_qa_company.qendpoint.core.util.string;

import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.*;

public class TypedLiteralCompactStringTest {
	@Test
	public void typedTest() {
		ByteString xsdInt = ByteString.of("<http://www.w3.org/2001/XMLSchema#integer>");

		TypedLiteralCompactString t1 = new TypedLiteralCompactString(ByteString.of(12345678), xsdInt);
		TypedLiteralCompactString t2 = new TypedLiteralCompactString(ByteString.of(12345678.0), xsdInt);
		TypedLiteralCompactString t3 = new TypedLiteralCompactString(ByteString.of(BigDecimal.valueOf(12345678)),
				xsdInt);
		ByteString t4 = ByteString.of("\"12345678\"^^<http://www.w3.org/2001/XMLSchema#integer>");
		TypedLiteralCompactString t5 = new TypedLiteralCompactString(ByteString.of(BigDecimal.valueOf(12345678)), null);
		ByteString t6 = ByteString.of("\"12345678\"");

		assertEquals("t1 != t2", t1, t2);
		assertEquals("t1 != t3", t1, t3);
		assertEquals("t2 != t2", t2, t2);
		assertEquals("t2 != t1", t2, t1);
		assertEquals("t3 != t3", t3, t3);
		assertEquals("t3 != t1", t3, t1);
		assertEquals("t2 != t1", t2, t3);
		assertEquals("t2 != t1", t3, t2);
		assertEquals("t1 != t4", t1, t4);
		assertEquals("t4 != t1", t4, t1);
		assertEquals("t5 != t6", t5, t6);
		assertEquals("t6 != t5", t6, t5);
	}

}
