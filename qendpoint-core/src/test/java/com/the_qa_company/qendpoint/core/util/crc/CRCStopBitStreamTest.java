package com.the_qa_company.qendpoint.core.util.crc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;

import org.junit.Test;

public class CRCStopBitStreamTest {
	private static final int SENTINEL_1 = 34;
	private static final int SENTINEL_2 = 12;
	private static final int SENTINEL_3 = 27;

	private static final class CountingCRC implements CRC {
		private final CRC delegate;
		private long updatedBytes;

		private CountingCRC(CRC delegate) {
			this.delegate = delegate;
		}

		@Override
		public void update(byte[] buffer, int offset, int length) {
			updatedBytes += length;
			delegate.update(buffer, offset, length);
		}

		@Override
		public void update(com.the_qa_company.qendpoint.core.util.io.CloseMappedByteBuffer buffer, int offset,
				int length) {
			updatedBytes += length;
			delegate.update(buffer, offset, length);
		}

		@Override
		public void update(byte data) {
			updatedBytes++;
			delegate.update(data);
		}

		@Override
		public void writeCRC(OutputStream out) throws java.io.IOException {
			delegate.writeCRC(out);
		}

		@Override
		public int writeCRC(com.the_qa_company.qendpoint.core.util.io.CloseMappedByteBuffer buffer, int offset)
				throws java.io.IOException {
			return delegate.writeCRC(buffer, offset);
		}

		@Override
		public boolean readAndCheck(InputStream in) throws java.io.IOException {
			return delegate.readAndCheck(in);
		}

		@Override
		public boolean readAndCheck(com.the_qa_company.qendpoint.core.util.io.CloseMappedByteBuffer buffer, int offset)
				throws java.io.IOException {
			return delegate.readAndCheck(buffer, offset);
		}

		@Override
		public long getValue() {
			return delegate.getValue();
		}

		@Override
		public void reset() {
			delegate.reset();
		}

		@Override
		public int sizeof() {
			return delegate.sizeof();
		}

		@Override
		public int compareTo(CRC o) {
			return delegate.compareTo(o);
		}
	}

	@Test
	public void crcInputStream_doesNotUpdateCrcOnEofRead() throws Exception {
		CountingCRC crc = new CountingCRC(new CRC32());
		try (CRCInputStream in = new CRCInputStream(new ByteArrayInputStream(new byte[0]), crc)) {
			assertEquals(-1, in.read());
		}
		assertEquals("EOF must not update CRC", 0L, crc.updatedBytes);
	}

	@Test
	public void vbyte_fastInputInterfaceExists() throws Exception {
		Class.forName("com.the_qa_company.qendpoint.core.compact.integer.VByte$FastInput");
	}

	@Test
	public void vbyte_fastOutputInterfaceExists() throws Exception {
		Class.forName("com.the_qa_company.qendpoint.core.compact.integer.VByte$FastOutput");
	}

	@Test
	public void crcStopBitInputStream_readsCrcFromPrefetchedBuffer() throws Exception {
		byte[] bytes = createPayloadWithCrcAndSentinels();

		CountingCRC crc = new CountingCRC(new CRC32());
		try (InputStream rawIn = new ByteArrayInputStream(bytes)) {
			Class<?> cls = Class.forName("com.the_qa_company.qendpoint.core.util.io.CRCStopBitInputStream");
			Constructor<?> ctor = cls.getConstructor(InputStream.class, CRC.class, int.class);
			CRCInputStream in = (CRCInputStream) ctor.newInstance(rawIn, crc, 64);

			assertEquals(1L, VByte.decode(in));
			assertEquals(2L, VByte.decode(in));
			assertEquals(3L, VByte.decode(in));

			long updatedBeforeCrc = crc.updatedBytes;
			assertTrue(in.readCRCAndCheck());
			assertEquals("CRC digest bytes must not update CRC", updatedBeforeCrc, crc.updatedBytes);

			assertEquals(SENTINEL_1, in.read());
			assertEquals(SENTINEL_2, in.read());
			assertEquals(SENTINEL_3, in.read());
		}
	}

	@Test
	public void crcStopBitOutputStream_writeCrcDoesNotUpdateCrc() throws Exception {
		CountingCRC crc = new CountingCRC(new CRC32());
		ByteArrayOutputStream rawOut = new ByteArrayOutputStream();

		Class<?> cls = Class.forName("com.the_qa_company.qendpoint.core.util.crc.CRCStopBitOutputStream");
		Constructor<?> ctor = cls.getConstructor(OutputStream.class, CRC.class, int.class);
		CRCOutputStream out = (CRCOutputStream) ctor.newInstance(rawOut, crc, 16);

		VByte.encode(out, 1L);
		VByte.encode(out, 2L);
		VByte.encode(out, 3L);
		long updatedBeforeCrc = crc.updatedBytes;
		out.writeCRC();
		assertEquals("CRC digest bytes must not update CRC", updatedBeforeCrc, crc.updatedBytes);
		out.close();
	}

	private static byte[] createPayloadWithCrcAndSentinels() throws Exception {
		ByteArrayOutputStream byteStrOut = new ByteArrayOutputStream();
		CRCOutputStream crcOut = new CRCOutputStream(byteStrOut, new CRC32());
		VByte.encode(crcOut, 1L);
		VByte.encode(crcOut, 2L);
		VByte.encode(crcOut, 3L);
		crcOut.writeCRC();
		crcOut.close();

		byteStrOut.write(SENTINEL_1);
		byteStrOut.write(SENTINEL_2);
		byteStrOut.write(SENTINEL_3);
		return byteStrOut.toByteArray();
	}
}
