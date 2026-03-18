package com.the_qa_company.qendpoint.core.util.crc;

import com.the_qa_company.qendpoint.core.util.io.CloseMappedByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.CRC32C;

/**
 * Implementation of CRC32-C Algorithm. Width = 32 Poly = 0x1edc6f41 XorIn =
 * 0xffffffff ReflectIn = True XorOut = 0xffffffff ReflectOut = True
 *
 * @author mario.arias
 */
public class CRC32 implements CRC {
	private final CRC32C crc = new CRC32C();

	public CRC32() {
		reset();
	}

	@Override
	public void update(byte[] buffer, int offset, int length) {
		crc.update(buffer, offset, length);
	}

	@Override
	public void update(CloseMappedByteBuffer buffer, int offset, int length) {
		if (length <= 0) {
			return;
		}
		if (buffer.hasArray()) {
			crc.update(buffer.array(), buffer.arrayOffset() + offset, length);
			return;
		}
		int remaining = length;
		int position = offset;
		byte[] chunk = new byte[Math.min(length, 8192)];
		while (remaining > 0) {
			int count = Math.min(remaining, chunk.length);
			buffer.get(position, chunk, 0, count);
			crc.update(chunk, 0, count);
			position += count;
			remaining -= count;
		}
	}

	@Override
	public void update(byte data) {
		crc.update(data & 0xFF);
	}

	@Override
	public void writeCRC(OutputStream out) throws IOException {

		IOUtil.writeInt(out, (int) (crc.getValue() & 0xFFFFFFFFL));
	}

	@Override
	public int writeCRC(CloseMappedByteBuffer channel, int offset) throws IOException {
		IOUtil.writeInt(channel, offset, (int) (crc.getValue() & 0xFFFFFFFFL));
		return 4;
	}

	@Override
	public boolean readAndCheck(InputStream in) throws IOException {
		int readCRC = IOUtil.readInt(in);
		return readCRC == (int) (crc.getValue() & 0xFFFFFFFFL);
	}

	@Override
	public boolean readAndCheck(CloseMappedByteBuffer channel, int offset) {
		int readCRC = IOUtil.readInt(channel, offset);
		return readCRC == (int) (crc.getValue() & 0xFFFFFFFFL);
	}

	@Override
	public long getValue() {
		return crc.getValue() & 0xFFFFFFFFL;
	}

	@Override
	public void reset() {
		crc.reset();
	}

	@Override
	public int compareTo(CRC o) {
		if (o instanceof CRC32) {
			int other = ~((int) ((CRC32) o).crc.getValue());
			int current = ~((int) crc.getValue());
			return other - current;
		}
		throw new RuntimeException("Cannot compare CRC's of different types");
	}

	@Override
	public String toString() {
		return Long.toHexString(getValue() & 0xFFFFFFFFL);
	}

	@Override
	public int sizeof() {
		return 4;
	}
}
