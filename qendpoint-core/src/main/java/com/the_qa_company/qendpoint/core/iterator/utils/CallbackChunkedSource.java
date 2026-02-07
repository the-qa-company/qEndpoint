package com.the_qa_company.qendpoint.core.iterator.utils;

import com.the_qa_company.qendpoint.core.util.concurrent.ExceptionSupplier;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;

/**
 * Bridges push-based producers (callbacks) into a pull-based, chunk-based
 * source without per-element queue nodes. The producer batches items into
 * chunks and enqueues one {@link SizedSupplier} per chunk; consumers pull
 * chunks via {@link #get()}.
 */
public final class CallbackChunkedSource<T> implements ExceptionSupplier<SizedSupplier<T>, IOException>, Closeable {
	public interface Producer<T> {
		void run(Consumer<T> sink) throws Exception;
	}

	private static final Object END = new Object();

	public static <T> CallbackChunkedSource<T> start(Producer<T> producer, ToLongFunction<T> sizer,
			long chunkBudgetBytes, int maxElementsPerChunk, int queueCapacityChunks) {
		return new CallbackChunkedSource<>(producer, sizer, chunkBudgetBytes, maxElementsPerChunk, queueCapacityChunks);
	}

	private final BlockingQueue<Object> queue;
	private final ToLongFunction<T> sizer;
	private final long chunkBudgetBytes;
	private final int maxElementsPerChunk;

	private final Thread producerThread;

	private volatile boolean end;
	private volatile boolean closed;
	private volatile Throwable failure;

	private CallbackChunkedSource(Producer<T> producer, ToLongFunction<T> sizer, long chunkBudgetBytes,
			int maxElementsPerChunk, int queueCapacityChunks) {
		if (chunkBudgetBytes <= 0) {
			throw new IllegalArgumentException("chunkBudgetBytes must be > 0");
		}
		if (maxElementsPerChunk <= 0) {
			throw new IllegalArgumentException("maxElementsPerChunk must be > 0");
		}
		if (queueCapacityChunks <= 0) {
			throw new IllegalArgumentException("queueCapacityChunks must be > 0");
		}
		this.queue = new ArrayBlockingQueue<>(queueCapacityChunks);
		this.sizer = sizer;
		this.chunkBudgetBytes = chunkBudgetBytes;
		this.maxElementsPerChunk = maxElementsPerChunk;

		this.producerThread = new Thread(() -> runProducer(producer), "CallbackChunkedSource-producer");
		this.producerThread.setDaemon(true);
		this.producerThread.start();
	}

	@Override
	public SizedSupplier<T> get() throws IOException {
		if (end) {
			throwIfFailure();
			return null;
		}

		final Object item;
		try {
			item = queue.take();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IOException(e);
		}

		if (item == END) {
			end = true;
			reinsertEnd();
			throwIfFailure();
			return null;
		}

		@SuppressWarnings("unchecked")
		SizedSupplier<T> supplier = (SizedSupplier<T>) item;
		return supplier;
	}

	@Override
	public void close() throws IOException {
		closed = true;
		producerThread.interrupt();
		reinsertEnd();
		if (Thread.currentThread() == producerThread) {
			return;
		}
		try {
			producerThread.join();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		throwIfFailure();
	}

	private void runProducer(Producer<T> producer) {
		try {
			ChunkBuilder builder = new ChunkBuilder();
			producer.run(builder);
			builder.flush();
		} catch (Throwable t) {
			if (!closed && !Thread.currentThread().isInterrupted()) {
				failure = t;
			}
		} finally {
			putEnd();
		}
	}

	private void putEnd() {
		try {
			queue.put(END);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	private void reinsertEnd() {
		if (queue.offer(END)) {
			return;
		}
		try {
			queue.put(END);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	private void throwIfFailure() throws IOException {
		if (failure == null) {
			return;
		}
		if (failure instanceof IOException ioe) {
			throw ioe;
		}
		throw new IOException(failure);
	}

	private final class ChunkBuilder implements Consumer<T> {
		private ArrayList<T> buffer = new ArrayList<>();
		private long size;

		@Override
		public void accept(T value) {
			Objects.requireNonNull(value, "CallbackChunkedSource doesn't support null elements");

			if (closed || Thread.currentThread().isInterrupted()) {
				throw new RuntimeException(new InterruptedException());
			}

			buffer.add(value);
			size += sizer.applyAsLong(value);

			if (buffer.size() >= maxElementsPerChunk || size >= chunkBudgetBytes) {
				flush();
			}
		}

		private void flush() {
			if (buffer.isEmpty()) {
				return;
			}

			ArrayList<T> out = buffer;
			long outSize = size;
			buffer = new ArrayList<>();
			size = 0;

			try {
				queue.put(new ListSizedSupplier<>(out, outSize));
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
		}
	}
}
