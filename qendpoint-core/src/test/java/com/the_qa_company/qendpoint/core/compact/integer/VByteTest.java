package com.the_qa_company.qendpoint.core.compact.integer;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;

import com.the_qa_company.qendpoint.core.util.Mutable;
import com.the_qa_company.qendpoint.core.util.string.ReplazableString;
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
	public void testSizeOf() throws IOException {
		for (int i : new int[] { 0, 1, (1 << 7) - 1, 1 << 7, 1 << 10, (1 << 14) - 1, 1 << 14, 1 << 16 }) {
			ByteArrayOutputStream osu = new ByteArrayOutputStream();
			ByteArrayOutputStream oss = new ByteArrayOutputStream();

			VByte.encode(osu, i);
			VByte.encodeSigned(oss, i);

			assertEquals(osu.toByteArray().length, VByte.sizeOf(i));
			assertEquals(oss.toByteArray().length, VByte.sizeOfSigned(i));
		}
	}

	@Test
	public void testString() {
		for (int i : new int[] { 1, (1 << 7) - 1, 1 << 7, 1 << 10, (1 << 14) - 1, 1 << 14, 1 << 16 }) {
			ReplazableString rs = new ReplazableString(16);

			VByte.encodeStr(rs, i);

			Mutable<Long> val = new Mutable<>(0L);
			assertEquals(rs.length(), VByte.decodeStr(rs, 0, val));
			assertEquals(i, val.getValue().intValue());
		}
	}

	@Test
	public void testStringsNP() {
		for (int i = 1; i <= 0x100; i++) {
			ReplazableString rs = new ReplazableString(16);

			VByte.encodeStr(rs, i);

			// check correct value
			Mutable<Long> val = new Mutable<>(0L);
			assertEquals(rs.length(), VByte.decodeStr(rs, 0, val));
			assertEquals(i, val.getValue().intValue());

			// test that we didn't compress a \0
			for (int j = 0; j < rs.length(); j++) {
				assertNotEquals('\0', rs.charAt(j));
			}
		}
	}
}
