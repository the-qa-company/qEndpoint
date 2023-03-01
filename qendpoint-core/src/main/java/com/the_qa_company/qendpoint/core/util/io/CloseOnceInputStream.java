package com.the_qa_company.qendpoint.core.util.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class CloseOnceInputStream extends InputStream {
	private final InputStream stream;
	private boolean closed;

	public CloseOnceInputStream(InputStream stream) {
		this.stream = stream;
	}

	@Override
	public void close() throws IOException {
		if (!closed) {
			stream.close();
			closed = true;
		}
		super.close();
	}

	@Override
	public int read() throws IOException {
		return stream.read();
	}

	@Override
	public int read(byte[] b) throws IOException {
		return stream.read(b);
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return stream.read(b, off, len);
	}

	@Override
	public byte[] readAllBytes() throws IOException {
		return stream.readAllBytes();
	}

	@Override
	public byte[] readNBytes(int len) throws IOException {
		return stream.readNBytes(len);
	}

	@Override
	public int readNBytes(byte[] b, int off, int len) throws IOException {
		return stream.readNBytes(b, off, len);
	}

	@Override
	public long skip(long n) throws IOException {
		return stream.skip(n);
	}

	@Override
	public void skipNBytes(long n) throws IOException {
		stream.skipNBytes(n);
	}

	@Override
	public int available() throws IOException {
		return stream.available();
	}

	@Override
	public void mark(int readlimit) {
		stream.mark(readlimit);
	}

	@Override
	public void reset() throws IOException {
		stream.reset();
	}

	@Override
	public boolean markSupported() {
		return stream.markSupported();
	}

	@Override
	public long transferTo(OutputStream out) throws IOException {
		return stream.transferTo(out);
	}
}
