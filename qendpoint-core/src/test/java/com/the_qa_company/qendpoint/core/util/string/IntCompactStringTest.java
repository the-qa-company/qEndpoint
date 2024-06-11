package com.the_qa_company.qendpoint.core.util.string;

import org.junit.Test;

import static org.junit.Assert.*;

public class IntCompactStringTest {
	@Test
	public void intCompactTest() {
		IntCompactString cs = new IntCompactString(0);
		for (int vl : new int[] {42, 806, 23456789, -2345678, -1, 0}) {
			// test set
			cs.setValue(vl);
			assertNull("buffer init", cs.buffer);
			// test using value itself
			assertEquals("bad val (long)", vl, cs.longValue());
			assertEquals("bad val (decimal)", vl, cs.decimalValue().doubleValue(), 0.001);
			assertEquals("bad val (double)", vl, cs.doubleValue(), 0.001);
			assertEquals("bad clone", cs, cs.copy());
			// no init
			assertNull("buffer init", cs.buffer);
			assertEquals("bad val str", String.valueOf(vl), cs.toString());
			assertNotNull("buffer not init", cs.buffer);
			// test buffer
			byte[] buff = cs.getBuffer();
			assertArrayEquals("bad buff", String.valueOf(vl).getBytes(ByteStringUtil.STRING_ENCODING), buff);
			// test copy
			assertEquals("bad clone", cs, cs.copy());
			assertEquals("bad clone", 0, cs.compareTo(cs.copyAppend("")));
			CompactString cs2 = new CompactString(buff);
			assertEquals("bad copy", 0, cs.compareTo(cs2));
			assertEquals("bad hash", cs.hashCode(), cs2.hashCode());
			assertEquals("bad long", cs.longValue(), cs2.longValue());
			assertEquals("bad double", cs.doubleValue(), cs2.doubleValue(), 0.001);
			assertEquals("bad decimal", cs.decimalValue().doubleValue(), cs2.decimalValue().doubleValue(), 0.001);
		}
	}

}