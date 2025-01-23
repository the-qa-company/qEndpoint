package com.the_qa_company.qendpoint.core.util.io;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

/**
 * Reader to read bit stream, see {@link BitStreamWriter} to write bit streams.
 *
 * @author Antoine Willerval
 */
public class BitStreamReader implements Closeable {
	private final InputStream is;
	private final boolean closeEnd;

	private long current;
	private long offset = 0;
	private boolean needRead = true;

	public BitStreamReader(InputStream is) {
		this(is, true);
	}

	public BitStreamReader(InputStream is, boolean closeEnd) {
		this.is = is;
		this.closeEnd = closeEnd;
	}

	public void read(byte[] buffer) throws IOException {
		read(buffer, 0, buffer.length);
	}

	public long getPosition() {
		return offset;
	}

	public void read(byte[] buffer, int offset, int length) throws IOException {
		int l = length;

		int off = offset;
		while (l >= 8) {
			long num = readNumber(64);

			buffer[off] = (byte) (num & 0xFF);
			buffer[off + 1] = (byte) ((num >>> 8) & 0xFF);
			buffer[off + 2] = (byte) ((num >>> 16) & 0xFF);
			buffer[off + 3] = (byte) ((num >>> 24) & 0xFF);
			buffer[off + 4] = (byte) ((num >>> 32) & 0xFF);
			buffer[off + 5] = (byte) ((num >>> 40) & 0xFF);
			buffer[off + 6] = (byte) ((num >>> 48) & 0xFF);
			buffer[off + 7] = (byte) ((num >>> 56) & 0xFF);

			l -= 8;
			off += 8;
		}

		while (l > 0) {
			buffer[off++] = (byte) readNumber(8);
			l--;
		}
	}

	public long readSignedNumber(int bits) throws IOException {
		long l = readNumber(bits);
		if (bits == 64 || (l & (1L << (bits - 1))) == 0) {
			return l;
		}

		// add 1 to sign the number
		return (-1L << bits) | l;
	}

	public long readNumber(int bits) throws IOException {
		if (needRead) {
			current = IOUtil.readLong(is);
			needRead = false;
		}
		long v = (current >>> (offset & 0x3F)) & (bits == 64 ? -1 : ~(-1L << bits));

		if (offset + bits >>> 6 != offset >>> 6) {
			// we need to shift

			int bits2 = (int) ((offset + bits) & 0x3F);

			if (bits2 != 0) {
				current = IOUtil.readLong(is);

				// between 2 longs, we need to add the data
				v |= (current & ~(-1L << bits2)) << (bits - bits2);
			} else {
				needRead = true;
			}
		}
		offset += bits;
		return v;
	}

	public boolean readBit() throws IOException {
		return readNumber(1) == 1;
	}

	public byte readByte() throws IOException {
		return (byte) readNumber(8);
	}

	@Override
	public void close() throws IOException {
		if (closeEnd) {
			is.close();
		}
	}
}
