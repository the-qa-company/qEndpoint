package com.the_qa_company.qendpoint.core.util.disk;

import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.util.io.BigMappedByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.Closeable;
import java.io.IOException;

public class BigBufferLongArray implements LongArray, Closeable {
	public static LongArray of(BigMappedByteBuffer buffer) {
		return new BigBufferLongArray(buffer);
	}

	private final BigMappedByteBuffer array;

	private BigBufferLongArray(BigMappedByteBuffer array) {
		this.array = array;
	}

	@Override
	public long get(long index) {
		if (index < 0 || index * 8 >= array.capacity()) {
			throw new IndexOutOfBoundsException(index + " < 0 || " + index + " > " + array.capacity() / 8);
		}
		try {
			return IOUtil.readLong(index * 8, array);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void set(long index, long value) {
		throw new NotImplementedException();
	}

	@Override
	public long length() {
		return array.capacity() / 8;
	}

	@Override
	public int sizeOf() {
		return 64;
	}

	@Override
	public void resize(long newSize) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public void clear() {
		throw new NotImplementedException();
	}

	@Override
	public void close() throws IOException {
		array.clean();
	}
}
