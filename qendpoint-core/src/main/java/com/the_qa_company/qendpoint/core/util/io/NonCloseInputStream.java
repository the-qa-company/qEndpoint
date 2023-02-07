package com.the_qa_company.qendpoint.core.util.io;

import java.io.IOException;
import java.io.InputStream;

/**
 * Intercepts calls to close() to make sure that when passing an InputStream to
 * third party code, they never close the stream. The stream can be closed using
 * the original InputStream close(), or by calling doClose();
 *
 * @author mario.arias
 */

public class NonCloseInputStream extends InputStream {

	private final InputStream in;

	/**
	 * @param input
	 */
	public NonCloseInputStream(InputStream input) {
		this.in = input;
	}

	@Override
	public int read() throws IOException {
		return in.read();
	}

	@Override
	public int read(byte[] b) throws IOException {
		return in.read(b);
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return in.read(b, off, len);
	}

	@Override
	public long skip(long n) throws IOException {
		return in.skip(n);
	}

	@Override
	public void close() throws IOException {
		// NOT CLOSE!!
	}

	public void doClose() throws IOException {
		// We are sure, do close.
		in.close();
	}

	@Override
	public void mark(int readlimit) {
		in.mark(readlimit);
	}

	@Override
	public boolean markSupported() {
		return in.markSupported();
	}

	@Override
	public void reset() throws IOException {
		in.reset();
	}

}
