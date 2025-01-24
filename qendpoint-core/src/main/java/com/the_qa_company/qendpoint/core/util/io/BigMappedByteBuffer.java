package com.the_qa_company.qendpoint.core.util.io;

import java.io.IOException;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class BigMappedByteBuffer {
	static long maxBufferSize = Integer.MAX_VALUE;

	/**
	 * create a BigMappedByteBuffer of multiple
	 * {@link FileChannel#map(FileChannel.MapMode, long, long)} call
	 *
	 * @param filename name to track the filename
	 * @param ch       the File channel
	 * @param mode     the mode
	 * @param position the position in the file
	 * @param size     the size of the buffer, can be higher than
	 *                 {@link Integer#MAX_VALUE}
	 * @return BigMappedByteBuffer
	 * @throws IOException if we can't call
	 *                     {@link FileChannel#map(FileChannel.MapMode, long, long)}
	 */
	public static BigMappedByteBuffer ofFileChannel(String filename, FileChannel ch, FileChannel.MapMode mode,
			long position, long size) throws IOException {
		int bufferCount = (int) ((size - 1) / maxBufferSize) + 1;
		BigMappedByteBuffer buffer = new BigMappedByteBuffer(null, new ArrayList<>());
		for (int i = 0; i < bufferCount; i++) {
			long mapSize;

			if (i == bufferCount - 1 && size % maxBufferSize != 0) {
				mapSize = size % maxBufferSize;
			} else {
				mapSize = maxBufferSize;
			}
			buffer.buffers.add(IOUtil.mapChannel(filename, ch, mode, position + (long) i * maxBufferSize, mapSize));
		}
		return buffer;
	}

	private final BigMappedByteBuffer parent;
	private final List<CloseMappedByteBuffer> buffers;

	/**
	 * cat multiple buffers
	 *
	 * @param parent  the parent buffer for force
	 * @param buffers the buffers
	 */
	private BigMappedByteBuffer(BigMappedByteBuffer parent, List<CloseMappedByteBuffer> buffers) {
		this.buffers = buffers;
		this.parent = parent;
	}

	private BigMappedByteBuffer(BigMappedByteBuffer other, Function<CloseMappedByteBuffer, CloseMappedByteBuffer> map) {
		this(other, other.buffers.stream().map(map).collect(Collectors.toList()));
	}

	List<CloseMappedByteBuffer> getBuffers() {
		return buffers;
	}

	/**
	 * set the byte order of the buffer
	 *
	 * @param order the order
	 */
	public void order(ByteOrder order) {
		buffers.forEach(b -> b.order(order));
	}

	/**
	 * @return the capacity of the big buffer
	 */

	long capacity = -1;

	public long capacity() {
		if (capacity != -1) {
			return capacity;
		}

		long sum = buffers.stream().mapToLong(CloseMappedByteBuffer::capacity).sum();
		capacity = sum;
		return sum;
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

		return buffers.get(buffer).get(inBufferIndex);
	}

	/**
	 * set the position of the buffer
	 *
	 * @param position the position
	 */
	public void position(long position) {
		int mid = getBufferIndex(position);
		for (int i = 0; i < mid; i++) {
			buffers.get(i).position((int) maxBufferSize);
		}
		buffers.get(mid).position(getBufferOffset(position));
		for (int i = mid + 1; i < buffers.size(); i++) {
			buffers.get(i).position(0);
		}
	}

	/**
	 * @return the position
	 */
	public long position() {
		long pos = 0;
		for (CloseMappedByteBuffer b : buffers) {
			pos += b.position();
			if (b.hasRemaining())
				break;
		}
		return pos;
	}

	/**
	 * Forces any changes made to this buffer's content to be written to the
	 * storage device containing the mapped file.
	 *
	 * @see MappedByteBuffer#force()
	 */
	public void force() {
		if (parent != null) {
			parent.force();
		}
		for (CloseMappedByteBuffer b : buffers) {
			// maybe we are in a duplicated buffer
			if (b.getInternalBuffer() instanceof MappedByteBuffer) {
				b.force();
			}
		}
	}

	/**
	 * @return duplicate the buffer
	 */
	public BigMappedByteBuffer duplicate() {
		return new BigMappedByteBuffer(this, b -> new CloseMappedByteBuffer(null, b.duplicate(), true));
	}

	/**
	 * @return if we have remaining byte(s) to read
	 */
	public boolean hasRemaining() {
		return position() < capacity();
	}

	/**
	 * @return a byte, update the position
	 */
	public byte get() {
		return buffers.get(getBufferIndex(position())).get();
	}

	/**
	 * rewind the buffer
	 */
	public void rewind() {
		buffers.forEach(CloseMappedByteBuffer::rewind);
	}

	/**
	 * read a particular number of bytes in the buffer
	 *
	 * @param dst    the destination array
	 * @param offset the offset in the offset
	 * @param length the length to read
	 */
	public void get(byte[] dst, int offset, int length) {
		final long position = position();
		int buffer1 = getBufferIndex(position);
		int buffer2 = getBufferIndex(position + length - 1);

		if (buffer1 == buffer2) {
			// all the bytes are in the same buffer
			CloseMappedByteBuffer b = buffers.get(buffer1);
			b.get(dst, offset, length);
		} else {
			// we are using 2 buffers
			CloseMappedByteBuffer b1 = buffers.get(buffer1);
			CloseMappedByteBuffer b2 = buffers.get(buffer2);

			int toRead = b1.capacity() - getBufferOffset(position);

			b1.get(dst, offset, toRead);
			b2.get(dst, offset + toRead, length - toRead);
		}
	}

	/**
	 * read a particular number of bytes in the buffer
	 *
	 * @param dst      the destination array
	 * @param position buffer position
	 * @param offset   the offset in the offset
	 * @param length   the length to read
	 */
	public void get(byte[] dst, long position, int offset, int length) {
		int buffer1 = getBufferIndex(position);
		int buffer2 = getBufferIndex(position + length - 1);

		int r1 = getBufferOffset(position);

		if (buffer1 == buffer2) {
			// all the bytes are in the same buffer
			CloseMappedByteBuffer b = buffers.get(buffer1);
			b.get(r1, dst, offset, length);
		} else {
			// we are using 2 buffers
			CloseMappedByteBuffer b1 = buffers.get(buffer1);
			CloseMappedByteBuffer b2 = buffers.get(buffer2);

			int toRead = b1.capacity() - getBufferOffset(position);

			b1.get(r1, dst, offset, toRead);
			b2.get(0, dst, offset + toRead, length - toRead);
		}
	}

	/**
	 * clean the buffer
	 */
	public void clean() throws IOException {
		IOUtil.closeAll(getBuffers());
	}
}
