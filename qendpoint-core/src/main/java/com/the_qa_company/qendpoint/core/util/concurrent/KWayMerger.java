package com.the_qa_company.qendpoint.core.util.concurrent;

import com.the_qa_company.qendpoint.core.iterator.utils.AsyncIteratorFetcher;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Object to perform k-Way-Merge using an {@link AsyncIteratorFetcher}
 *
 * @param <E> merge object type
 * @author Antoine Willerval
 */
public class KWayMerger<E, S extends Supplier<E>> {
	private static final AtomicInteger ID = new AtomicInteger();
	private final int k;
	private final AsyncIteratorFetcher<E> iteratorFetcher;
	private final KWayMergerImpl<E, S> impl;
	private final Worker[] workers;
	private final AtomicLong pathId = new AtomicLong();
	private final CloseSuppressPath workLocation;
	private final Lock dataLock = new ReentrantLock();
	private boolean started;
	private boolean end;
	private final HeightTree<Chunk> chunks = new HeightTree<>();
	private Throwable throwable;

	/**
	 * kwaymerger
	 *
	 * @param workLocation location to store the chunks
	 * @param syncSupplier the element supplier
	 * @param impl         implementation of {@link KWayMergerImpl} to
	 *                     create/handle the chunks
	 * @param workers      the number of workers
	 * @param k            the k in the k-way merge
	 */
	public KWayMerger(CloseSuppressPath workLocation, AsyncIteratorFetcher<E> syncSupplier, KWayMergerImpl<E, S> impl,
			int workers, int k) throws KWayMergerException {
		this.workLocation = workLocation;
		this.iteratorFetcher = syncSupplier;
		this.impl = impl;
		this.k = k;

		try {
			workLocation.mkdirs();
		} catch (IOException e) {
			throw new KWayMergerException("Can't create workLocation directory!", e);
		}

		this.workers = new Worker[workers];
		int id = ID.incrementAndGet();
		for (int i = 0; i < workers; i++) {
			this.workers[i] = new Worker("KWayMerger#" + id + "Worker#" + i, this);
		}
	}

	/**
	 * start all the workers
	 */
	public void start() {
		if (started) {
			throw new IllegalArgumentException("The KWayMerger was already started and can't be reused!");
		}
		started = true;
		for (Worker w : workers) {
			w.start();
		}
	}

	private void exception(Throwable t) {
		if (throwable != null) {
			throwable.addSuppressed(t);
		} else {
			throwable = t;
		}
		for (Worker w : workers) {
			w.interrupt();
		}
	}

	/**
	 * wait the result and return it (if any), this method isn't thread safe and
	 * can't be called twice
	 *
	 * @return optional of the result
	 * @throws InterruptedException wait interupption
	 * @throws KWayMergerException  exception while merging
	 */
	public Optional<CloseSuppressPath> waitResult() throws InterruptedException, KWayMergerException {
		if (!started) {
			throw new IllegalArgumentException("The KWayMerger hasn't been started!");
		}
		for (Worker w : workers) {
			w.join();
		}
		if (throwable != null) {
			if (throwable instanceof Error) {
				throw (Error) throwable;
			}
			if (throwable instanceof RuntimeException) {
				throw (RuntimeException) throwable;
			}
			if (throwable instanceof KWayMergerException) {
				throw (KWayMergerException) throwable;
			}
			throw new KWayMergerException(throwable);
		}

		if (chunks.size() > 1) {
			throw new KWayMergerException("Chunk size is above 1! " + chunks.size());
		}

		List<Chunk> all = chunks.getAll(1);
		return all.isEmpty() ? Optional.empty() : Optional.of(all.get(0).getPath());
	}

	@FunctionalInterface
	private interface KWayMergerRunnable {
		void run() throws KWayMergerException;
	}

	/**
	 * implementation to handle the chunks
	 *
	 * @param <E> chunk types
	 */
	public interface KWayMergerImpl<E, S extends Supplier<E>> {
		/**
		 * create a chunk from a flux, the flux is the returned by
		 * {@link #newStopFlux(Supplier)}
		 *
		 * @param flux   flux to handle
		 * @param output output to write the chunk
		 * @throws KWayMergerException any exception returned by this method's
		 *                             implementation
		 */
		void createChunk(S flux, CloseSuppressPath output) throws KWayMergerException;

		/**
		 * merge chunks together into a new chunk
		 *
		 * @param inputs the chunks
		 * @param output the output chunk
		 * @throws KWayMergerException any exception returned by this method's
		 *                             implementation
		 */
		void mergeChunks(List<CloseSuppressPath> inputs, CloseSuppressPath output) throws KWayMergerException;

		/**
		 * create a flux from another one to tell when to stop
		 *
		 * @param flux the flux
		 * @return the new flux
		 * @throws KWayMergerException any exception returned by this method's
		 *                             implementation
		 */
		S newStopFlux(Supplier<E> flux) throws KWayMergerException;
	}

	/**
	 * @return a unique path into the work location
	 */
	private CloseSuppressPath getPath() {
		return workLocation.resolve("f-" + pathId.incrementAndGet());
	}

	/**
	 * @return thread safe method to get a method to handle or null if no other
	 *         tasks are required
	 */
	private KWayMergerRunnable getTask() {
		dataLock.lock();
		try {
			if (end) {
				if (chunks.size() <= 1) {
					return null;
				}

				List<Chunk> all = chunks.getAll(k);

				return new MergeTask(all);
			}

			List<Chunk> chunkList = chunks.getMax(k);

			if (chunkList != null) {
				return new MergeTask(chunkList);
			}

			return new GetTask();
		} finally {
			dataLock.unlock();
		}
	}

	private class MergeTask implements KWayMergerRunnable {
		private final List<Chunk> chunks;

		public MergeTask(List<Chunk> chunks) {
			assert !chunks.isEmpty() : "empty chunks";
			this.chunks = chunks;
		}

		@Override
		public void run() throws KWayMergerException {
			int chunk = chunks.stream().mapToInt(Chunk::getHeight).max().orElseThrow() + 1;
			CloseSuppressPath mergec = getPath();
			List<CloseSuppressPath> paths = chunks.stream().map(Chunk::getPath)
					.collect(Collectors.toUnmodifiableList());
			impl.mergeChunks(paths, mergec);
			try {
				IOUtil.closeAll(paths);
			} catch (IOException e) {
				throw new KWayMergerException("Can't close end merge files", e);
			}
			dataLock.lock();
			try {
				KWayMerger.this.chunks.addElement(new Chunk(chunk, mergec), chunk);
			} finally {
				dataLock.unlock();
			}
		}
	}

	private class GetTask implements KWayMergerRunnable {

		@Override
		public void run() throws KWayMergerException {
			CloseSuppressPath chunk = getPath();
			S flux = impl.newStopFlux(iteratorFetcher);
			impl.createChunk(flux, chunk);
			dataLock.lock();
			try {
				end = iteratorFetcher.isEnd();
				Chunk newChunk = new Chunk(1, chunk);
				chunks.addElement(newChunk, newChunk.getHeight());
			} finally {
				dataLock.unlock();
			}
		}
	}

	private static class Chunk {
		private final int height;
		private final CloseSuppressPath path;

		public Chunk(int height, CloseSuppressPath path) {
			this.height = height;
			this.path = path;
		}

		public int getHeight() {
			return height;
		}

		public CloseSuppressPath getPath() {
			return path;
		}
	}

	private static class Worker extends ExceptionThread {
		private final KWayMerger<?, ?> parent;

		public Worker(String name, KWayMerger<?, ?> parent) {
			super(name);
			this.parent = parent;
		}

		@Override
		public void runException() throws Exception {
			try {
				KWayMergerRunnable task;

				while (!isInterrupted() && (task = parent.getTask()) != null) {
					task.run();
				}
			} catch (Throwable t) {
				parent.exception(t);
				throw t;
			}
		}
	}

	/**
	 * Exception linked with the {@link KWayMerger}, this class isn't a
	 * {@link RuntimeException} because these exceptions should be seriously
	 * considered
	 *
	 * @author Antoine Willerval
	 */
	public static class KWayMergerException extends Exception {
		public KWayMergerException(String message) {
			super(message);
		}

		public KWayMergerException(String message, Throwable cause) {
			super(message, cause);
		}

		public KWayMergerException(Throwable cause) {
			super(cause);
		}
	}
}
