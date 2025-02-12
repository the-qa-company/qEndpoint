package com.the_qa_company.qendpoint.core.storage;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * The class implements a buffered output stream. By setting up such an output
 * stream, an application can write bytes to the underlying output stream
 * without necessarily causing a call to the underlying system for each byte
 * written.
 *
 * @author Arthur van Hoff
 * @since 1.0
 */
public class TempBuffOut extends FilterOutputStream {
	private static final int DEFAULT_INITIAL_BUFFER_SIZE = 512;
	private static final int DEFAULT_MAX_BUFFER_SIZE = 8192;

	/**
	 * The internal buffer where data is stored.
	 */
	protected byte[] buf;

	/**
	 * The number of valid bytes in the buffer. This value is always in the
	 * range {@code 0} through {@code buf.length}; elements {@code buf[0]}
	 * through {@code buf[count-1]} contain valid byte data.
	 */
	protected int count;

	/**
	 * Max size of the internal buffer.
	 */
	private final int maxBufSize;

	/**
	 * Returns the buffer size to use when no output buffer size specified.
	 */
	private static int initialBufferSize() {
		return DEFAULT_MAX_BUFFER_SIZE;
	}

	/**
	 * Creates a new buffered output stream.
	 */
	private TempBuffOut(OutputStream out, int initialSize, int maxSize) {
		super(out);

		if (initialSize <= 0) {
			throw new IllegalArgumentException("Buffer size <= 0");
		}

		this.buf = new byte[maxSize];

		this.maxBufSize = maxSize;
	}

	/**
	 * Creates a new buffered output stream to write data to the specified
	 * underlying output stream.
	 *
	 * @param out the underlying output stream.
	 */
	public TempBuffOut(OutputStream out) {
		this(out, initialBufferSize(), DEFAULT_MAX_BUFFER_SIZE);
	}

	/**
	 * Creates a new buffered output stream to write data to the specified
	 * underlying output stream with the specified buffer size.
	 *
	 * @param out  the underlying output stream.
	 * @param size the buffer size.
	 * @throws IllegalArgumentException if size &lt;= 0.
	 */
	public TempBuffOut(OutputStream out, int size) {
		this(out, size, size);
	}

	/** Flush the internal buffer */
	private void flushBuffer() throws IOException {
		if (count > 0) {
			out.write(buf, 0, count);
			count = 0;
		}
	}

	/**
	 * Writes the specified byte to this buffered output stream.
	 *
	 * @param b the byte to be written.
	 * @throws IOException if an I/O error occurs.
	 */
	@Override
	public void write(int b) throws IOException {

		implWrite(b);

	}

	private void implWrite(int b) throws IOException {
		if (count >= buf.length) {
			flushBuffer();
		}
		buf[count++] = (byte) b;
	}

	/**
	 * Writes {@code len} bytes from the specified byte array starting at offset
	 * {@code off} to this buffered output stream.
	 * <p>
	 * Ordinarily this method stores bytes from the given array into this
	 * stream's buffer, flushing the buffer to the underlying output stream as
	 * needed. If the requested length is at least as large as this stream's
	 * buffer, however, then this method will flush the buffer and write the
	 * bytes directly to the underlying output stream. Thus redundant
	 * {@code TempBuffOut}s will not copy data unnecessarily.
	 *
	 * @param b   the data.
	 * @param off the start offset in the data.
	 * @param len the number of bytes to write.
	 * @throws IOException               if an I/O error occurs.
	 * @throws IndexOutOfBoundsException {@inheritDoc}
	 */
	@Override
	public void write(byte[] b, int off, int len) throws IOException {

		implWrite(b, off, len);

	}

	private void implWrite(byte[] b, int off, int len) throws IOException {
		if (len >= maxBufSize) {
			/*
			 * If the request length exceeds the max size of the output buffer,
			 * flush the output buffer and then write the data directly. In this
			 * way buffered streams will cascade harmlessly.
			 */
			flushBuffer();
			out.write(b, off, len);
			return;
		}
		if (len > buf.length - count) {
			flushBuffer();
		}
		System.arraycopy(b, off, buf, count, len);
		count += len;
	}

	/**
	 * Flushes this buffered output stream. This forces any buffered output
	 * bytes to be written out to the underlying output stream.
	 *
	 * @throws IOException if an I/O error occurs.
	 * @see java.io.FilterOutputStream#out
	 */
	@Override
	public void flush() throws IOException {

		implFlush();

	}

	private void implFlush() throws IOException {
		flushBuffer();
		out.flush();
	}
}
