package com.the_qa_company.qendpoint.client;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Output stream implementation to redirect a stream into 2 streams
 *
 * @author Antoine Willerval
 */
public class SplitStream extends OutputStream {
	/**
	 * create a split stream of streams
	 *
	 * @param streams the stream, an empty array will return a null stream, a
	 *                singleton array will return the element
	 * @return the split, null stream or first element
	 */
	public static OutputStream of(OutputStream... streams) {
		if (streams.length == 0) {
			return OutputStream.nullOutputStream();
		}
		if (streams.length == 1) {
			return streams[0];
		}
		OutputStream os = streams[0];

		for (int i = 1; i < streams.length; i++) {
			os = new SplitStream(os, streams[i]);
		}

		return os;
	}

	private final OutputStream stream;
	private final OutputStream other;

	private SplitStream(OutputStream stream, OutputStream other) {
		this.stream = stream;
		this.other = other;
	}

	@Override
	public void write(int b) throws IOException {
		stream.write(b);
		other.write(b);
	}

	@Override
	public void write(byte[] b) throws IOException {
		stream.write(b);
		other.write(b);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		stream.write(b, off, len);
		other.write(b, off, len);
	}

	@Override
	public void flush() throws IOException {
		stream.flush();
		other.flush();
	}

	@Override
	public void close() throws IOException {
		try {
			stream.close();
		} finally {
			other.close();
		}
	}
}
