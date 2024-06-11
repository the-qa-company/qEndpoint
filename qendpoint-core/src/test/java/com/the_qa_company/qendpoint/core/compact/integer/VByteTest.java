package com.the_qa_company.qendpoint.core.compact.integer;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;

import org.junit.Test;

public class VByteTest {

	@Test
	public void testMaxSize() throws IOException {

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		VByte.encode(out, Long.MAX_VALUE);
//		System.out.println("Size of "+Long.MAX_VALUE+" => "+out.size());

		long val = VByte.decode(new ByteArrayInputStream(out.toByteArray()));
//		System.out.println("Value back: "+val);
		assertEquals(Long.MAX_VALUE, val);

		out = new ByteArrayOutputStream();
		VByte.encode(out, Integer.MAX_VALUE);
//		System.out.println("Size of "+Integer.MAX_VALUE+" => "+out.size());
		val = VByte.decode(new ByteArrayInputStream(out.toByteArray()));
		assertEquals(Integer.MAX_VALUE, val);
//		System.out.println("Value back: "+val);

	}

	@Test(expected = IllegalArgumentException.class)
	public void testMoreCharactersThanNeeded() throws IOException {
		byte[] arr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		long val = VByte.decode(new ByteArrayInputStream(arr));
		System.out.println("VAL: " + val);
		fail("Exception not thrown");
	}

	@Test(expected = EOFException.class)
	public void testEOF() throws IOException {
		byte[] arr = { 0, 0 };
		long val = VByte.decode(new ByteArrayInputStream(arr));
		System.out.println("VAL: " + val);
		fail("Exception not thrown");
	}

	@Test
	public void testInvVB() throws IOException {
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		ByteArrayOutputStream os2 = new ByteArrayOutputStream();

		final long val = 0x642707;

		VByte.encodeInv(os, val);
		VByte.encode(os2, val);

		byte[] ba = os.toByteArray();
		byte[] ba2 = os2.toByteArray();

		// the inv va is the reversed, so ba = reverse(ba2)
		assertEquals(ba.length, ba2.length);
		for (int i = 0; i < ba.length; i++) {
			assertEquals("bad index: " + i, ba[i], ba2[ba2.length - 1 - i]);
		}

		long vald1 = VByte.decodeInv(ba, ba.length - 1);
		long vald2 = VByte.decode(new ByteArrayInputStream(ba2));
		assertEquals("invalid decode", vald1, val);
		assertEquals("invalid inv decode", vald1, vald2);
	}
}
