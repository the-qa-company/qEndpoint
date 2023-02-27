package com.the_qa_company.qendpoint.core.util.io;

import java.io.IOException;
import java.io.OutputStream;

public class CloseOnceOutputStream extends OutputStream {
	@Override
	public void write(int b) throws IOException {
		stream.write(b);
	}

	@Override
	public void write(byte[] b) throws IOException {
		stream.write(b);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		stream.write(b, off, len);
	}

	@Override
	public void flush() throws IOException {
		stream.flush();
	}

	private final OutputStream stream;
	private boolean closed;

	public CloseOnceOutputStream(OutputStream stream) {
		this.stream = stream;
	}

	@Override
	public void close() throws IOException {
		if (!closed) {
			stream.close();
			closed = true;
		}
	}
}
