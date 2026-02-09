package com.the_qa_company.qendpoint.core.iterator.utils;

import com.the_qa_company.qendpoint.core.util.concurrent.ExceptionSupplier;
import org.apache.commons.io.FileUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.ToLongFunction;

/**
 * Adapts an {@link Iterator} into a thread-safe, pull-based chunk supplier.
 * Each call to {@link #get()} buffers a worker-owned chunk under a small lock,
 * so workers contend once per chunk rather than once per element.
 */
public final class IteratorChunkedSource<E> implements ExceptionSupplier<SizedSupplier<E>, IOException>, Closeable {
	public static <E> IteratorChunkedSource<E> of(Iterator<E> it, ToLongFunction<E> sizer, long chunkBudgetBytes,
			Function<E, E> copier) {
		return new IteratorChunkedSource<>(it, sizer, chunkBudgetBytes, copier);
	}

	private final Iterator<E> iterator;
	private final ToLongFunction<E> sizer;
	private final long chunkBudgetBytes;
	private final Function<E, E> copier;

	private final Lock lock = new ReentrantLock();
	private boolean end;

	private IteratorChunkedSource(Iterator<E> iterator, ToLongFunction<E> sizer, long chunkBudgetBytes,
			Function<E, E> copier) {
		if (chunkBudgetBytes <= 0) {
			throw new IllegalArgumentException("chunkBudgetBytes must be > 0");
		}
		this.iterator = iterator;
		this.sizer = sizer;
		this.chunkBudgetBytes = chunkBudgetBytes;
		this.copier = copier;
	}

	static int bufferSize = 4;

	@Override
	public SizedSupplier<E> get() {
		lock.lock();
		try {
			if (end) {
				return null;
			}

			ArrayList<E> buffer = new ArrayList<>(bufferSize);
			long size = 0;

			while (iterator.hasNext()) {
				E e = iterator.next();
				if (copier != null) {
					e = copier.apply(e);
				}
				buffer.add(e);
				size += sizer.applyAsLong(e);

				if (size >= chunkBudgetBytes) {
					break;
				}
			}

			bufferSize = Math.min(32 * 1024 * 1023, Math.max(bufferSize, buffer.size()));

			if (buffer.isEmpty()) {
				end = true;
				return null;
			}

			return new ListSizedSupplier<>(buffer, size);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void close() throws IOException {
		if (iterator instanceof Closeable closeable) {
			closeable.close();
			return;
		}
		if (iterator instanceof AutoCloseable closeable) {
			try {
				closeable.close();
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
	}
}
