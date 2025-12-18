package com.the_qa_company.qendpoint.core.util.io;

import java.io.IOException;

public class BigMappedByteBufferInputStream extends BufferInputStream {
	final BigMappedByteBuffer buf;
	long offset;
	long end;

	public BigMappedByteBufferInputStream(BigMappedByteBuffer buf) {
		this(buf, 0, buf.capacity());
	}

	public BigMappedByteBufferInputStream(BigMappedByteBuffer buf, long offset, long len) {
		this.buf = buf;
		this.offset = offset;
		end = offset + len;
	}

	@Override
	public long remaining() {
		return end - offset;
	}

	public boolean hasRemaining() {
		return offset < end;
	}

	@Override
	public synchronized int read() throws IOException {
		if (!hasRemaining()) {
			return -1;
		}
		return Byte.toUnsignedInt(buf.get(offset++));
	}

	@Override
	public synchronized int read(byte[] bytes, int off, int len) throws IOException {
		if (!hasRemaining()) {
			return -1;
		}
		// Read only what's left
		len = (int) Math.min(len, end - offset);
		buf.get(bytes, offset, off, len);
		offset += len;
		return len;
	}

	@Override
	public long skip(long n) {
		n = Math.min(n, end - offset);
		offset += n;
		return n;
	}

	@Override
	public boolean canRead(long len) {
		return offset + len <= end;
	}
}
