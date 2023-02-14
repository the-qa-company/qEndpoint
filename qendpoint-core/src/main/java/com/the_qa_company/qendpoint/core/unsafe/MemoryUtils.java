package com.the_qa_company.qendpoint.core.unsafe;

import com.the_qa_company.qendpoint.core.util.concurrent.ExceptionThread;

import java.lang.ref.Cleaner;
import java.lang.reflect.Field;
import java.util.stream.IntStream;

/**
 * Unsafe memory utilities
 *
 * @author Antoine Willerval
 */
public class MemoryUtils {
	/**
	 * Unsafe object
	 */
	private static final sun.misc.Unsafe UNSAFE;
	/**
	 * Cleaner
	 */
	private static final Cleaner CLEANER = Cleaner.create();
	/**
	 * pointer to a zero buffer of size {@link #BUFFER_SIZE}
	 */
	private static final long ZERO_BUFFER;
	/**
	 * pointer to a minus one buffer of size {@link #BUFFER_SIZE}
	 */
	private static final long MINUS_ONE_BUFFER;
	/**
	 * Object to clean the ZERO/MINUS buffers
	 */
	private static final Object BUFFER_PARENT = new Object() {};
	/**
	 * number of bits in the buffer size
	 */
	private static final int BUFFER_SIZE_BITS = 12; // 2**12 = 4096
	/**
	 * number of bytes in the buffer for {@link #MINUS_ONE_BUFFER} and
	 * {@link #ZERO_BUFFER}, is equal to 2 to the power of
	 * {@link #BUFFER_SIZE_BITS}.
	 */
	private static final int BUFFER_SIZE;
	/**
	 * max size of a Java array before switching to unsafe array, non-final for
	 * debug
	 */
	static int maxArraySize = Integer.MAX_VALUE >> 1;
	/**
	 * threshold before switching to parallel set in memset, non-final for debug
	 */
	static long thresholdParallelSizeSet = (long) maxArraySize * 10;

	static {
		try {
			Field f = Class.forName("sun.misc.Unsafe").getDeclaredField("theUnsafe");
			f.setAccessible(true);
			UNSAFE = (sun.misc.Unsafe) f.get(null);
			if (UNSAFE == null) {
				throw new NullPointerException("Unsafe value is null!");
			}
		} catch (NullPointerException | ClassNotFoundException | IllegalAccessException | IllegalArgumentException
				| NoSuchFieldException | SecurityException e) {
			throw new Error("Can't get field value sun.misc.Unsafe", e);
		}

		if (BUFFER_SIZE_BITS < 3) {
			throw new Error("BUFFER_SIZE_BITS can't be lower than 3!");
		}
		BUFFER_SIZE = 1 << BUFFER_SIZE_BITS;

		// allocate the buffers and directly bind them to their parents

		ZERO_BUFFER = UNSAFE.allocateMemory(BUFFER_SIZE);
		bindPointerTo(ZERO_BUFFER, BUFFER_PARENT);

		MINUS_ONE_BUFFER = UNSAFE.allocateMemory(BUFFER_SIZE);
		bindPointerTo(MINUS_ONE_BUFFER, BUFFER_PARENT);

		// now we can set the values
		for (long i = 0; i < (BUFFER_SIZE >> 3); i++) {
			UNSAFE.putLong(ZERO_BUFFER + i * 8, 0);
			// we can use -1 because -1L = 0xFFFF_FFFF_FFFF_FFFF, so full of
			// 0xFF bytes
			UNSAFE.putLong(MINUS_ONE_BUFFER + i * 8, -1L);
		}
	}

	/**
	 * @return unsafe object, be careful and gentle with it
	 */
	public static sun.misc.Unsafe getUnsafe() {
		return UNSAFE;
	}

	/**
	 * @return cleaner object
	 */
	public static Cleaner getCleaner() {
		return CLEANER;
	}

	/**
	 * @return max size of an array
	 */
	public static int getMaxArraySize() {
		return maxArraySize;
	}

	/**
	 * allocate memory
	 *
	 * @param size   number of elements
	 * @param sizeOf size of an element
	 * @return pointer
	 */
	public static long malloc(long size, int sizeOf) {
		return malloc(size * sizeOf);
	}

	/**
	 * allocate memory
	 *
	 * @param size size to allocate
	 * @return pointer
	 */
	public static long malloc(long size) {
		return UNSAFE.allocateMemory(size);
	}

	/**
	 * free a pointer from the memory
	 *
	 * @param ptr pointer
	 */
	public static void free(long ptr) {
		UNSAFE.freeMemory(ptr);
	}

	/**
	 * bind this pointer to an object
	 *
	 * @param pointer pointer
	 * @param parent  parent object
	 */
	public static void bindPointerTo(long pointer, Object parent) {
		CLEANER.register(parent, new CleanerObject(pointer));
	}

	private static void memorySet0(long ptr, long size, byte value) {
		// fast version using buffer
		if (value == -1) {
			memorySetBuffer(ptr, size, MINUS_ONE_BUFFER);
			return;
		}
		if (value == 0) {
			memorySetBuffer(ptr, size, ZERO_BUFFER);
			return;
		}
		int uv = value & 0xFF;
		long lvalue = uv | (uv << 8L) | (uv << 16L) | ((long) uv << 24L) | ((long) uv << 32) | ((long) uv << 40)
				| ((long) uv << 48) | ((long) uv << 56);
		long addr = ptr;
		long lend = ptr + (size & ~7) - 1;
		while (addr < lend) {
			// use put long to reduce call to putByte
			UNSAFE.putLong(addr, lvalue);
			addr += 8;
		}
		// end padding
		while (addr < ptr + size) {
			UNSAFE.putByte(addr, value);
			addr++;
		}
	}

	private static void memorySetBuffer(long ptr, long size, long buffer) {
		long addr = ptr;
		long tow = size;
		while (tow > 0) {
			long len;
			if (tow > BUFFER_SIZE) {
				len = BUFFER_SIZE;
			} else {
				len = tow;
			}
			UNSAFE.copyMemory(buffer, addr, len);
			tow -= len;
			addr += len;
		}
	}

	/**
	 * copy a memory block from one part of the memory to another one
	 *
	 * @param dst  destination block start
	 * @param src  source block start
	 * @param size size to copy
	 */
	public static void memcpy(long dst, long src, long size) {
		UNSAFE.copyMemory(src, dst, size);
	}

	/**
	 * set all the bytes at an address
	 *
	 * @param ptr   pointer
	 * @param size  bytes to allocate
	 * @param value value
	 */
	public static void memset(long ptr, long size, byte value) {
		if (size > thresholdParallelSizeSet) {
			int processors = Runtime.getRuntime().availableProcessors();
			long blockSize = size / processors + 1;
			try {
				ExceptionThread.async("memorySetThread", IntStream.range(0, processors).mapToObj(processor -> {
					long start = blockSize * processor;
					long end = Math.min(size, blockSize * (processor + 1));
					return (ExceptionThread.ExceptionRunnable) () -> memorySet0(ptr + start, end - start, value);
				}).toArray(ExceptionThread.ExceptionRunnable[]::new)).startAll().joinAndCrashIfRequired();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		} else {
			memorySet0(ptr, size, value);
		}
	}

	private MemoryUtils() {
		throw new IllegalArgumentException("Can't init utils class");
	}

	private static class CleanerObject implements Runnable {
		private long ptr;

		private CleanerObject(long ptr) {
			this.ptr = ptr;
		}

		@Override
		public void run() {
			if (ptr != 0) {
				free(ptr);
				ptr = 0;
			}
		}
	}
}
