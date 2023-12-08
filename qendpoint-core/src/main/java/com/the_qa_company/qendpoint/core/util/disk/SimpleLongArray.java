package com.the_qa_company.qendpoint.core.util.disk;

import com.the_qa_company.qendpoint.core.compact.sequence.DynamicSequence;
import com.the_qa_company.qendpoint.core.compact.sequence.Sequence;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Iterator;

public class SimpleLongArray implements DynamicSequence {

	public static DynamicSequence of(int size) {
		return wrapper(new long[size]);
	}

	public static DynamicSequence wrapper(long[] array) {
		return new SimpleLongArray(array);
	}

	private SimpleLongArray(long[] array) {
		this.array = array;
	}

	private long[] array;

	@Override
	public void add(Iterator<Long> elements) {

	}

	@Override
	public long get(long index) {
		if (index < 0 || index >= array.length) {
			throw new IndexOutOfBoundsException(index + " < 0 || " + index + " > " + array.length);
		}
		return array[(int) index];
	}

	@Override
	public long getNumberOfElements() {
		return 0;
	}

	@Override
	public long size() {
		return 0;
	}

	@Override
	public void set(long index, long value) {
		if (index < 0 || index >= array.length) {
			throw new IndexOutOfBoundsException(index + " < 0 || " + index + " > " + array.length);
		}
		array[(int) index] = value;
	}

	@Override
	public void append(long value) {
		set(length(), value);
	}

	@Override
	public void trimToSize() {
		// ignore
	}

	@Override
	public void aggressiveTrimToSize() {
		// ignore
	}

	@Override
	public long length() {
		return array.length;
	}

	@Override
	public int sizeOf() {
		return 64;
	}

	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public void load(InputStream input, ProgressListener listener) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public String getType() {
		return HDTVocabulary.SEQ_TYPE_INT64;
	}

	@Override
	public void resize(long newSize) {
		if (Integer.MAX_VALUE - 5 <= newSize) {
			throw new IllegalArgumentException("Can't rezise to a size bigger than " + (Integer.MAX_VALUE - 5));
		}
		long[] newArray = new long[(int) newSize];

		System.arraycopy(this.array, 0, newArray, 0, Math.min(this.array.length, newArray.length));
		this.array = newArray;
	}

	@Override
	public void clear() {
		Arrays.fill(array, 0);
	}

	@Override
	public void close() throws IOException {
		// nothing to do
	}
}
