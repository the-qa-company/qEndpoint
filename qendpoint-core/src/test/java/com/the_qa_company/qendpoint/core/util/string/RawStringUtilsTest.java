package com.the_qa_company.qendpoint.core.util.string;

import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Random;

import static org.junit.Assert.*;

public class RawStringUtilsTest {
	@Test
	public void serialTest() {
		String s1 = "\"test\"";
		String s2 = "\"test\"^^<http://example.org/#type1>";
		String s3 = "\"test\"@en";
		String s4 = "<http://example.org/#type1>";

		String s1s = "N\"test\"";
		String s2s = "^<http://example.org/#type1>\0\"test\"";
		String s3s = "@en\0\"test\"";
		String s4s = "#<http://example.org/#type1>";

		assertEquals(ByteString.of(s1s), RawStringUtils.convertToRawString(s1));
		assertEquals(ByteString.of(s2s), RawStringUtils.convertToRawString(s2));
		assertEquals(ByteString.of(s3s), RawStringUtils.convertToRawString(s3));
		assertEquals(ByteString.of(s4s), RawStringUtils.convertToRawString(s4));

		assertEquals(ByteString.of(s1s), RawStringUtils.convertToRawString(RawStringUtils.convertFromRawString(s1s)));
		assertEquals(ByteString.of(s2s), RawStringUtils.convertToRawString(RawStringUtils.convertFromRawString(s2s)));
		assertEquals(ByteString.of(s3s), RawStringUtils.convertToRawString(RawStringUtils.convertFromRawString(s3s)));
		assertEquals(ByteString.of(s4s), RawStringUtils.convertToRawString(RawStringUtils.convertFromRawString(s4s)));

		assertEquals(ByteString.of(s1), RawStringUtils.convertFromRawString(RawStringUtils.convertToRawString(s1)));
		assertEquals(ByteString.of(s2), RawStringUtils.convertFromRawString(RawStringUtils.convertToRawString(s2)));
		assertEquals(ByteString.of(s3), RawStringUtils.convertFromRawString(RawStringUtils.convertToRawString(s3)));
		assertEquals(ByteString.of(s4), RawStringUtils.convertFromRawString(RawStringUtils.convertToRawString(s4)));

		String v1 = "\"123456789\"^^" + RawStringUtils.XSD_INTEGER_DT;
		String v2 = "\"1.23456789E8\"^^" + RawStringUtils.XSD_DOUBLE_DT;
		String v3 = "\"123456789\"^^" + RawStringUtils.XSD_DECIMAL_DT;
		String v4 = "\"123456789\"";

		// type things

		assertEquals(ByteString.of(v1), RawStringUtils.convertFromRawString(RawStringUtils.convertToRawString(v1)));
		assertEquals(ByteString.of(v2), RawStringUtils.convertFromRawString(RawStringUtils.convertToRawString(v2)));
		assertEquals(ByteString.of(v3), RawStringUtils.convertFromRawString(RawStringUtils.convertToRawString(v3)));
		assertEquals(ByteString.of(v4), RawStringUtils.convertFromRawString(RawStringUtils.convertToRawString(v4)));
	}

	@Test
	public void compareTest() {
		Random rnd = new Random(4579);

		for (long i = 0; i < 1000; i++) {
			int i1 = rnd.nextInt();
			int i2 = rnd.nextInt();

			ByteString si1 = RawStringUtils.convertToRawString(RawStringUtils.integerLiteral(i1));
			ByteString si2 = RawStringUtils.convertToRawString(RawStringUtils.integerLiteral(i2));
			assertEquals(Integer.signum(Long.compare(i1, i2)),
					Integer.signum(RawStringUtils.compareRawString(si1, si2)));
		}

		for (long i = 0; i < 1000; i++) {
			double d1 = rnd.nextDouble();
			double d2 = rnd.nextDouble();

			ByteString sd1 = RawStringUtils.convertToRawString(RawStringUtils.floatLiteral(d1));
			ByteString sd2 = RawStringUtils.convertToRawString(RawStringUtils.floatLiteral(d2));
			assertEquals(Integer.signum(Double.compare(d1, d2)),
					Integer.signum(RawStringUtils.compareRawString(sd1, sd2)));
		}

		for (long i = 0; i < 1000; i++) {
			BigDecimal d1 = BigDecimal.valueOf(rnd.nextDouble());
			BigDecimal d2 = BigDecimal.valueOf(rnd.nextDouble());

			ByteString sd1 = RawStringUtils.convertToRawString(RawStringUtils.decimalLiteral(d1));
			ByteString sd2 = RawStringUtils.convertToRawString(RawStringUtils.decimalLiteral(d2));
			assertEquals(Integer.signum(d1.compareTo(d2)), Integer.signum(RawStringUtils.compareRawString(sd1, sd2)));
		}
	}

	@Test
	public void rawTypeTest() {
		assertSame(RawStringUtils.XSD_DECIMAL_DT, RawStringUtils.rawType(RawStringUtils.convertToRawString(RawStringUtils.decimalLiteral(new BigDecimal(34566)))));
		assertSame(RawStringUtils.XSD_DOUBLE_DT, RawStringUtils.rawType(RawStringUtils.convertToRawString(RawStringUtils.floatLiteral(34566.0232))));
		assertSame(RawStringUtils.XSD_INTEGER_DT, RawStringUtils.rawType(RawStringUtils.convertToRawString(RawStringUtils.integerLiteral(34566))));
		ByteString type = ByteString.of("<http://example.org/#type>");
		assertEquals(type, RawStringUtils.rawType(RawStringUtils.convertToRawString(new TypedLiteralCompactString(ByteString.of("azerty"), type))));
		assertSame(LiteralsUtils.NO_DATATYPE, RawStringUtils.rawType(RawStringUtils.convertToRawString(ByteString.of("http://example.org/#test"))));
		assertSame(LiteralsUtils.LITERAL_LANG_TYPE, RawStringUtils.rawType(RawStringUtils.convertToRawString(ByteString.of("\"zqdzqd\"@fr"))));

	}
}
