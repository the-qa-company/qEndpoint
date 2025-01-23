package com.the_qa_company.qendpoint.core.util.io;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.*;

public class BitStreamTest {
	@Test
	public void baseTest() throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();

		final long largeLong = 0x1234567890123456L;
		final long rndSeed = 0x34;

		try (BitStreamWriter writer = new BitStreamWriter(out)) {
			writer.writeLong(largeLong, 64);
			writer.writeBit(true);
			writer.writeBit(false);
			writer.writeBit(true);
			writer.writeBit(false);
			writer.writeByte((byte) 12);
			writer.writeLong(3, 2);
			writer.writeLong(-10, 7);
			writer.writeLong(10, 7);
			writer.writeLong(1, 1);
			writer.writeLong(456, 10);
			writer.writeBit(true);
			for (int i = 1; i <= 64; i++) {
				long v = i == 64 ? largeLong : (largeLong & ~(-1L << i));
				writer.writeLong(v, i);
			}

			Random rnd = new Random(rndSeed);

			for (int i = 0; i < 1000; i++) {
				int len = 1 + rnd.nextInt(64);

				long val = rnd.nextLong();
				if (len != 64) {
					val &= ~(-1L << len);
				}

				writer.writeLong(val, len);
			}
		}

		ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());

		try (BitStreamReader reader = new BitStreamReader(in)) {
			assertEquals(largeLong, reader.readNumber(64));
			assertTrue(reader.readBit());
			assertFalse(reader.readBit());
			assertTrue(reader.readBit());
			assertFalse(reader.readBit());
			assertEquals((byte) 12, reader.readByte());
			assertEquals(3, reader.readNumber(2));
			assertEquals(-10, reader.readSignedNumber(7));
			assertEquals(10, reader.readNumber(7));
			assertEquals(1, reader.readNumber(1));
			assertEquals(456, reader.readNumber(10));
			assertTrue(reader.readBit());
			for (int i = 1; i <= 64; i++) {
				long v = i == 64 ? largeLong : (largeLong & ~(-1L << i));
				long r = reader.readNumber(i);
				assertEquals("bad read for i=" + i + "\n0x" + Long.toHexString(v) + "\n0x" + Long.toHexString(r)
						+ "\n at 0x" + Long.toHexString(reader.getPosition()), v, r);
			}

			Random rnd = new Random(rndSeed);

			for (int i = 0; i < 1000; i++) {
				int len = 1 + rnd.nextInt(64);

				long val = rnd.nextLong();
				if (len != 64) {
					val &= ~(-1L << len);
				}

				assertEquals(val, reader.readNumber(len));
			}
		}
	}

	@Test
	public void emptyTest() throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();

		new BitStreamWriter(out).close();

		ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());

		new BitStreamReader(in).close();
	}
}
