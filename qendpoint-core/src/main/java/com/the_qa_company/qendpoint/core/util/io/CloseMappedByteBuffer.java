package com.the_qa_company.qendpoint.core.util.io;

import java.io.Closeable;
import java.nio.*;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CloseMappedByteBuffer implements Closeable {
	public static CloseMappedByteBuffer wrap(ByteBuffer buffer) {
		return new CloseMappedByteBuffer(buffer);
	}

	private static final AtomicLong ID_GEN = new AtomicLong();
	private static final Map<Long, Throwable> MAP_TEST_MAP = new HashMap<>();
	private static boolean mapTest = false;

	static void markMapTest() {
		mapTest = true;
		MAP_TEST_MAP.clear();
	}

	static void crashMapTest() {
		mapTest = false;
		if (!MAP_TEST_MAP.isEmpty()) {
			AssertionError re = new AssertionError(MAP_TEST_MAP.size() + " MAP(S) NOT CLOSED!");

			MAP_TEST_MAP.entrySet().stream().sorted(Comparator.comparingLong(Map.Entry::getKey))
					.map(Map.Entry::getValue).forEach(re::addSuppressed);

			throw re;
		}
	}

	private final long id = ID_GEN.incrementAndGet();
	private final ByteBuffer buffer;
	private final boolean duplicated;

	CloseMappedByteBuffer(ByteBuffer buffer) {
		this.buffer = buffer;
		this.duplicated = true;
	}

	CloseMappedByteBuffer(String filename, ByteBuffer buffer, boolean duplicated) {
		this.duplicated = duplicated;
		this.buffer = buffer;
		if (mapTest && !duplicated) {
			synchronized (MAP_TEST_MAP) {
				MAP_TEST_MAP.put(id, new Throwable("MAP " + filename + "#" + id + "|" + buffer));
			}
		}
	}

	@Override
	public void close() {
		if (mapTest && !duplicated) {
			synchronized (MAP_TEST_MAP) {
				MAP_TEST_MAP.remove(id);
			}
		}
		IOUtil.cleanBuffer(buffer);
	}

	public boolean isLoaded() {
		return ((MappedByteBuffer) buffer).isLoaded();
	}

	public MappedByteBuffer load() {
		return ((MappedByteBuffer) buffer).load();
	}

	public MappedByteBuffer force() {
		assert buffer instanceof MappedByteBuffer;
		return ((MappedByteBuffer) buffer).force();
	}

	public ByteBuffer position(int newPosition) {
		return buffer.position(newPosition);
	}

	public ByteBuffer limit(int newLimit) {
		return buffer.limit(newLimit);
	}

	public ByteBuffer mark() {
		return buffer.mark();
	}

	public ByteBuffer reset() {
		return buffer.reset();
	}

	public ByteBuffer clear() {
		return buffer.clear();
	}

	public ByteBuffer flip() {
		return buffer.flip();
	}

	public ByteBuffer rewind() {
		return buffer.rewind();
	}

	public ByteBuffer slice() {
		return buffer.slice();
	}

	public ByteBuffer duplicate() {
		return buffer.duplicate();
	}

	public ByteBuffer asReadOnlyBuffer() {
		return buffer.asReadOnlyBuffer();
	}

	public byte get() {
		return buffer.get();
	}

	public ByteBuffer put(byte b) {
		return buffer.put(b);
	}

	public byte get(int index) {
		return buffer.get(index);
	}

	public ByteBuffer put(int index, byte b) {
		return buffer.put(index, b);
	}

	public ByteBuffer put(int index, byte[] bytes) {
		return buffer.put(index, bytes);
	}

	public ByteBuffer put(int index, byte[] bytes, int offset, int length) {
		return buffer.put(index, bytes, offset, length);
	}

	public ByteBuffer get(int index, byte[] dst, int offset, int length) {
		return buffer.get(index, dst, offset, length);
	}

	public ByteBuffer get(byte[] dst, int offset, int length) {
		return buffer.get(dst, offset, length);
	}

	public ByteBuffer get(byte[] dst) {
		return buffer.get(dst);
	}

	public ByteBuffer put(ByteBuffer src) {
		return buffer.put(src);
	}

	public ByteBuffer put(byte[] src, int offset, int length) {
		return buffer.put(src, offset, length);
	}

	public ByteBuffer put(byte[] src) {
		return buffer.put(src);
	}

	public boolean hasArray() {
		return buffer.hasArray();
	}

	public byte[] array() {
		return buffer.array();
	}

	public int arrayOffset() {
		return buffer.arrayOffset();
	}

	public ByteBuffer compact() {
		return buffer.compact();
	}

	public boolean isDirect() {
		return buffer.isDirect();
	}

	public int compareTo(ByteBuffer that) {
		return buffer.compareTo(that);
	}

	public int mismatch(ByteBuffer that) {
		return buffer.mismatch(that);
	}

	public ByteOrder order() {
		return buffer.order();
	}

	public ByteBuffer order(ByteOrder bo) {
		return buffer.order(bo);
	}

	public int alignmentOffset(int index, int unitSize) {
		return buffer.alignmentOffset(index, unitSize);
	}

	public ByteBuffer alignedSlice(int unitSize) {
		return buffer.alignedSlice(unitSize);
	}

	public char getChar() {
		return buffer.getChar();
	}

	public ByteBuffer putChar(char value) {
		return buffer.putChar(value);
	}

	public char getChar(int index) {
		return buffer.getChar(index);
	}

	public ByteBuffer putChar(int index, char value) {
		return buffer.putChar(index, value);
	}

	public CharBuffer asCharBuffer() {
		return buffer.asCharBuffer();
	}

	public short getShort() {
		return buffer.getShort();
	}

	public ByteBuffer putShort(short value) {
		return buffer.putShort(value);
	}

	public short getShort(int index) {
		return buffer.getShort(index);
	}

	public ByteBuffer putShort(int index, short value) {
		return buffer.putShort(index, value);
	}

	public ShortBuffer asShortBuffer() {
		return buffer.asShortBuffer();
	}

	public int getInt() {
		return buffer.getInt();
	}

	public ByteBuffer putInt(int value) {
		return buffer.putInt(value);
	}

	public int getInt(int index) {
		return buffer.getInt(index);
	}

	public ByteBuffer putInt(int index, int value) {
		return buffer.putInt(index, value);
	}

	public IntBuffer asIntBuffer() {
		return buffer.asIntBuffer();
	}

	public long getLong() {
		return buffer.getLong();
	}

	public ByteBuffer putLong(long value) {
		return buffer.putLong(value);
	}

	public long getLong(int index) {
		return buffer.getLong(index);
	}

	public ByteBuffer putLong(int index, long value) {
		return buffer.putLong(index, value);
	}

	public LongBuffer asLongBuffer() {
		return buffer.asLongBuffer();
	}

	public float getFloat() {
		return buffer.getFloat();
	}

	public ByteBuffer putFloat(float value) {
		return buffer.putFloat(value);
	}

	public float getFloat(int index) {
		return buffer.getFloat(index);
	}

	public ByteBuffer putFloat(int index, float value) {
		return buffer.putFloat(index, value);
	}

	public FloatBuffer asFloatBuffer() {
		return buffer.asFloatBuffer();
	}

	public double getDouble() {
		return buffer.getDouble();
	}

	public ByteBuffer putDouble(double value) {
		return buffer.putDouble(value);
	}

	public double getDouble(int index) {
		return buffer.getDouble(index);
	}

	public ByteBuffer putDouble(int index, double value) {
		return buffer.putDouble(index, value);
	}

	public DoubleBuffer asDoubleBuffer() {
		return buffer.asDoubleBuffer();
	}

	public int capacity() {
		return buffer.capacity();
	}

	public int position() {
		return buffer.position();
	}

	public int limit() {
		return buffer.limit();
	}

	public int remaining() {
		return buffer.remaining();
	}

	public boolean hasRemaining() {
		return buffer.hasRemaining();
	}

	public boolean isReadOnly() {
		return buffer.isReadOnly();
	}

	public ByteBuffer getInternalBuffer() {
		return buffer;
	}
}
