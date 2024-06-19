package com.the_qa_company.qendpoint.core.unsafe;

import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.util.disk.LongArray;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Objects;

/**
 * long array switching to memory allocation if the size is too important
 *
 * @author Antoine Willerval
 */
public class UnsafeLongArray implements LongArray {
	private static final int SIZE_OF = Long.BYTES;

	/**
	 * copy a block in a source array into a destination array, same as
	 * {@link System#arraycopy(Object, int, Object, int, int)} with
	 * #UnsafeLongArray
	 *
	 * @param src     source array
	 * @param srcPos  source position
	 * @param dest    destination array
	 * @param destPos destination position
	 * @param length  length to copy
	 */
	public static void arraycopy(UnsafeLongArray src, long srcPos, UnsafeLongArray dest, long destPos, long length) {
		if (length < 0) {
			throw new IllegalArgumentException("Negative length");
		}
		if (length == 0 || src == dest) {
			return;
		}
		if (srcPos < 0 || srcPos + length > src.size()) {
			throw new IllegalArgumentException("source block out of bound!");
		}
		if (destPos < 0 || destPos + length > dest.size()) {
			throw new IllegalArgumentException("destination block out of bound!");
		}
		if (src.isUsingUnsafe() && dest.isUsingUnsafe()) {
			MemoryUtils.memcpy(dest.pointer + destPos * dest.sizeOf(), src.pointer + srcPos * src.sizeOf(), length);
		} else if (!src.isUsingUnsafe() && !dest.isUsingUnsafe()) {
			System.arraycopy(src.javaArray, (int) srcPos, dest.javaArray, (int) destPos, (int) length);
		} else {
			for (long i = 0; i < length; i++) {
				dest.set(destPos + i, src.get(srcPos + i));
			}
		}
	}

	/**
	 * wrap an array
	 *
	 * @param array array
	 * @return unsafe array
	 */
	public static UnsafeLongArray wrapper(long[] array) {
		return new UnsafeLongArray(array, false);
	}

	/**
	 * allocate and clone an array
	 *
	 * @param array array
	 * @return unsafe array
	 */
	public static UnsafeLongArray allocate(long[] array) {
		return new UnsafeLongArray(array, true);
	}

	/**
	 * allocate an array
	 *
	 * @param size size of the array
	 * @return unsafe array
	 */
	public static UnsafeLongArray allocate(long size) {
		return new UnsafeLongArray(size);
	}

	/**
	 * allocate an array
	 *
	 * @param size      size of the array
	 * @param initArray init the array with 0
	 * @return unsafe array
	 */
	public static UnsafeLongArray allocate(long size, boolean initArray) {
		return new UnsafeLongArray(size, initArray);
	}

	private final long pointer;
	private final long[] javaArray;
	private final long size;

	/**
	 * create an array filled with 0 of a particular size
	 *
	 * @param javaArray array
	 */
	private UnsafeLongArray(long[] javaArray, boolean copyArray) {
		this.size = javaArray.length;
		if (!copyArray) {
			pointer = 0;
			this.javaArray = Objects.requireNonNull(javaArray, "javaArray can't be null!");
		} else {
			if (size >= MemoryUtils.getMaxArraySize()) {
				// allocate the pointer
				pointer = MemoryUtils.malloc(size, SIZE_OF);
				// bind this pointer to this object
				MemoryUtils.bindPointerTo(pointer, this);
				this.javaArray = null;
				arraycopy(wrapper(javaArray), 0, this, 0, size);
			} else {
				this.javaArray = new long[(int) size];
				// clone the array
				System.arraycopy(javaArray, 0, this.javaArray, 0, javaArray.length);
				pointer = 0;
			}
		}
	}

	/**
	 * create an array filled with 0 of a particular size
	 *
	 * @param size size of the array
	 */
	private UnsafeLongArray(long size) {
		this(size, true);
	}

	/**
	 * create an array of a particular size
	 *
	 * @param size size of the array
	 * @param init initialize the array with 0s
	 */
	private UnsafeLongArray(long size, boolean init) {
		this.size = size;
		if (size >= MemoryUtils.getMaxArraySize()) {
			// allocate the pointer
			pointer = MemoryUtils.malloc(size, SIZE_OF);
			// bind this pointer to this object
			MemoryUtils.bindPointerTo(pointer, this);
			javaArray = null;
			if (init) {
				clear();
			}
		} else {
			javaArray = new long[(int) size];
			pointer = 0;
		}
	}

	/**
	 * clear the array
	 */
	@Override
	public void clear() {
		if (javaArray == null) {
			MemoryUtils.memset(pointer, size * sizeOf(), (byte) 0);
		} else {
			Arrays.fill(javaArray, 0);
		}
	}

	/**
	 * get a value from the array
	 *
	 * @param index index
	 * @return value
	 */
	@Override
	public long get(long index) {
		if (javaArray != null) {
			return javaArray[(int) index];
		}
		assert index >= 0 && index < size;
		return MemoryUtils.getUnsafe().getLong(pointer + index * SIZE_OF);
	}

	/**
	 * set a value in the array
	 *
	 * @param index index
	 * @param value value
	 */
	@Override
	public void set(long index, long value) {
		if (javaArray != null) {
			javaArray[(int) index] = value;
			return;
		}
		assert index >= 0 && index < size;
		MemoryUtils.getUnsafe().putLong(pointer + index * SIZE_OF, value);
	}

	public boolean isUsingUnsafe() {
		return javaArray == null;
	}

	/**
	 * @return size of the array
	 */
	public long size() {
		return size;
	}

	/**
	 * @return size of the array
	 */
	@Override
	public long length() {
		return size;
	}

	/**
	 * @return the size of an element in the array
	 */
	@Override
	public int sizeOf() {
		return 8;
	}

	@Override
	public void resize(long newSize) {
		throw new NotImplementedException("Can't resize UnsafeLongArray");
	}

	/**
	 * read a certain amount of elements inside this array
	 *
	 * @param input  input stream
	 * @param size   element to read
	 * @param offset array offset
	 * @throws IOException reading issues
	 */
	public void read(InputStream input, long size, long offset) throws IOException {
		if (offset + size > this.size) {
			throw new IndexOutOfBoundsException("Can't read more bytes than the array size");
		}

		for (long i = 0; i < size; i++) {
			set(offset + i, IOUtil.readLong(input));
		}
	}

	/**
	 * write a certain amount of elements from this array to a strea
	 *
	 * @param output output stream
	 * @param size   element to write
	 * @param offset array offset
	 * @throws IOException writing issues
	 */
	public void write(OutputStream output, long size, long offset) throws IOException {
		if (offset + size > this.size) {
			throw new IndexOutOfBoundsException("Can't write more bytes than the array size");
		}

		for (long i = 0; i < size; i++) {
			IOUtil.writeLong(output, get(offset + i));
		}
	}
}
