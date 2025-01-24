package com.the_qa_company.qendpoint.core.util.io;

import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.ByteStringUtil;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Reader to write bit stream, see {@link BitStreamReader} to read bit streams.
 * Streams sizes are 8 bytes aligned.
 *
 * @author Antoine Willerval
 */
public class BitStreamWriter implements Closeable {
	private final OutputStream os;
	private final boolean closeEnd;
	private long offset;
	private long current;

	public BitStreamWriter(OutputStream os) {
		this(os, true);
	}

	public BitStreamWriter(OutputStream os, boolean closeEnd) {
		this.os = os;
		this.closeEnd = closeEnd;
	}

	public void writeLong(long val, int bits) throws IOException {
		if (bits != 64) {
			val &= (-1L >>> (64 - bits)); // remove to part, it is mandatory for
											// negative numbers
		}
		current |= (val << (offset & 0x3F));

		if (((offset + bits) >>> 6) == (offset >>> 6)) {
			// we don't need to write the bits in the stream
			offset += bits;
			return;
		}

		IOUtil.writeLong(os, current);
		// maybe we have some data left
		current = (val >>> (64 - (offset & 0x3F))) & ~(-1L << ((offset + bits) & 0x3F));
		offset += bits;
	}

	public void writeBit(boolean val) throws IOException {
		writeLong(val ? 1 : 0, 1);
	}

	public void writeString(String str) throws IOException {
		writeByteArray(str.getBytes(ByteStringUtil.STRING_ENCODING));
		writeLong(0, 8);
	}

	public void writeString(ByteString str) throws IOException {
		writeByteArray(str.getBuffer(), 0, str.length());
		writeLong(0, 8);
	}

	public void writeByteArray(byte[] arr) throws IOException {
		writeByteArray(arr, 0, arr.length);
	}

	public void writeByteArray(byte[] arr, int offset, int len) throws IOException {
		for (int i = 0; i < len; i++) {
			writeLong((int) arr[offset + i] & 0xFF, 8);
		}
	}

	public void writeByte(byte v) throws IOException {
		writeLong(v & 0xFFL, 8);
	}

	@Override
	public void close() throws IOException {
		try {
			if ((offset & 0x3F) != 0) {
				// write end of the data
				writeLong(0, 64 - (int) (offset & 0x3F));
			}
		} finally {
			if (closeEnd) {
				os.close();
			}
		}
	}
}
