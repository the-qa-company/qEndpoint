package com.the_qa_company.qendpoint.core.util.io;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.listener.ListenerUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class BigByteBuffer {
	static final int BUFFER_SIZE = 1024 * 8;
	static int maxBufferSize = Integer.MAX_VALUE - 5;

	/**
	 * allocate a large byte buffer
	 *
	 * @param size the size to allocate
	 * @return BigByteBuffer
	 */
	public static BigByteBuffer allocate(long size) {
		if (size < 0) {
			throw new IllegalArgumentException("Can't allocate ByteBuffer with a negative size: " + size);
		}
		if (size == 0) {
			return new BigByteBuffer(List.of());
		}
		int bufferCount = (int) ((size - 1) / maxBufferSize + 1);

		List<byte[]> buffers = new ArrayList<>(bufferCount);

		int lastNodeSize = (int) (size % maxBufferSize);
		if (lastNodeSize == 0) {
			for (int i = 0; i < bufferCount; i++) {
				buffers.add(new byte[maxBufferSize]);
			}
		} else {
			for (int i = 0; i < bufferCount - 1; i++) {
				buffers.add(new byte[maxBufferSize]);
			}
			buffers.add(new byte[lastNodeSize]);
		}
		return new BigByteBuffer(buffers);
	}

	private final List<byte[]> buffers;

	/**
	 * cat multiple buffers
	 *
	 * @param buffers the buffers
	 */
	private BigByteBuffer(List<byte[]> buffers) {
		this.buffers = buffers;
	}

	List<byte[]> getBuffers() {
		return buffers;
	}

	/**
	 * @return the capacity of the big buffer
	 */
	public long size() {
		return buffers.stream().mapToLong(l -> l.length).sum();
	}

	private int getBufferOffset(long index) {
		return (int) (index % maxBufferSize);
	}

	private int getBufferIndex(long index) {
		return (int) (index / maxBufferSize);
	}

	/**
	 * get a byte at a particular index
	 *
	 * @param index the byte index
	 * @return byte
	 */
	public byte get(long index) {
		int buffer = getBufferIndex(index);
		int inBufferIndex = getBufferOffset(index);

		if (buffer < 0 || buffer >= buffers.size())
			throw new IndexOutOfBoundsException();

		return buffers.get(buffer)[inBufferIndex];
	}

	/**
	 * set a byte at a particular index
	 *
	 * @param index the byte index
	 * @param value the byte to set
	 */
	public void set(long index, byte value) {
		int buffer = getBufferIndex(index);
		int inBufferIndex = getBufferOffset(index);

		if (buffer < 0 || buffer >= buffers.size())
			throw new IndexOutOfBoundsException();

		buffers.get(buffer)[inBufferIndex] = value;
	}

	/**
	 * set a byte at a particular index
	 *
	 * @param index  the byte index
	 * @param value  the byte to set
	 * @param offset the offset in the array
	 * @param length the length in the array after the offset
	 */
	public void set(long index, byte[] value, int offset, int length) {
		int buffer1 = getBufferIndex(index);
		int buffer2 = getBufferIndex(index + length - 1);

		if (buffer1 == buffer2) {
			// same array
			byte[] b = buffers.get(buffer1);
			System.arraycopy(value, offset, b, getBufferOffset(index), length);
		} else {
			byte[] b1 = buffers.get(buffer1);
			byte[] b2 = buffers.get(buffer2);

			int toRead = b1.length - getBufferOffset(index);

			System.arraycopy(value, offset, b1, getBufferOffset(index), toRead);
			System.arraycopy(value, offset + toRead, b2, getBufferOffset(index + toRead), length - toRead);
		}
	}

	/**
	 * read a particular number of bytes in the buffer
	 *
	 * @param dst      the destination array
	 * @param position index to start reading
	 * @param offset   the offset in the offset
	 * @param length   the length to read
	 */
	public void get(byte[] dst, long position, int offset, int length) {
		int buffer1 = getBufferIndex(position);
		int buffer2 = getBufferIndex(position + length - 1);

		if (buffer1 == buffer2) {
			// all the bytes are in the same buffer
			byte[] b = buffers.get(buffer1);
			System.arraycopy(b, getBufferOffset(position), dst, offset, length);
		} else {
			// we are using 2 buffers
			byte[] b1 = buffers.get(buffer1);
			byte[] b2 = buffers.get(buffer2);

			int toRead = b1.length - getBufferOffset(position);

			System.arraycopy(b1, getBufferOffset(position), dst, offset, toRead);
			System.arraycopy(b2, getBufferOffset(position + toRead), dst, offset + toRead, length - toRead);
		}
	}

	/**
	 * read a stream into this ByteBuffer
	 *
	 * @param input    the input stream to read
	 * @param index    the index to start writing
	 * @param length   the length to read
	 * @param listener listener to notify the state
	 * @throws IOException any error with the stream
	 */
	public void readStream(InputStream input, long index, long length, ProgressListener listener) throws IOException {
		long remaining = length;
		long currentIndex = index;
		int b = getBufferIndex(index);
		ListenerUtil.notify(listener, "Reading buffer", 0, length);
		while (remaining > 0) {
			int offset = getBufferOffset(currentIndex);
			byte[] buffer = buffers.get(b);

			int read = (int) Math.min(buffer.length - offset, currentIndex + remaining);
			readStreamInto(input, buffer, offset, read, listener, currentIndex - index, length);
			remaining -= read;
			currentIndex += read;
			ListenerUtil.notify(listener, "Reading buffer", length - remaining, length);
			b++;
		}
	}

	private void readStreamInto(InputStream input, byte[] dst, int start, int length, ProgressListener listener,
			long offset, long end) throws IOException {
		int nRead;
		int pos = 0;

		while ((nRead = input.read(dst, start, length - pos)) > 0) {
			pos += nRead;
			ListenerUtil.notify(listener, "Reading buffer", pos + offset, end);
		}
		if (pos != length) {
			throw new IOException("EOF while reading array from InputStream");
		}
	}

	/**
	 * Write a part of this byte buffer into a stream
	 *
	 * @param stream   the stream to fill
	 * @param offset   the offset to start
	 * @param length   the length to write
	 * @param listener listener to notify the state
	 * @throws IOException exception while writing into the output stream
	 */
	public void writeStream(OutputStream stream, long offset, long length, ProgressListener listener)
			throws IOException {
		byte[] buffer = new byte[BUFFER_SIZE];

		long end = offset + length;
		long write = 0;
		ListenerUtil.notify(listener, "Writing buffer", 0, length);
		for (long index = offset; index < end; index += BUFFER_SIZE) {
			int toWrite = (int) Math.min(end - index, BUFFER_SIZE);
			get(buffer, index, 0, toWrite);
			stream.write(buffer, 0, toWrite);
			write += toWrite;
			ListenerUtil.notify(listener, "Writing buffer", write, length);
		}
	}
}
