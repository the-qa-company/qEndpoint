package com.the_qa_company.qendpoint.core.util.concurrent;

import com.the_qa_company.qendpoint.core.util.concurrent.KWayMerger.KWayMergerException;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * K-way merge where "source data" is not a single shared element supplier, but
 * a supplier of per-chunk fluxes. Each chunk flux is consumed by a single
 * worker. This avoids per-element contention and lets each worker own a whole
 * chunk end-to-end.
 */
public class KWayMergerChunked<E, S extends Supplier<E>> {

	private static final AtomicInteger ID = new AtomicInteger();

	private final int k;
	private final int maxConcurrentMerges;
	private final ExceptionSupplier<S, IOException> chunkSupplier;
	private final KWayMergerChunkedImpl<E, S> impl;
	private final Worker[] workers;

	private final AtomicLong pathId = new AtomicLong();
	private final CloseSuppressPath workLocation;

	private final ReentrantLock dataLock = new ReentrantLock();
	private final Condition taskAvailable = dataLock.newCondition();
	private boolean started;
	private boolean end;
	private final HeightTree<Chunk> chunks = new HeightTree<>();
	private int activeMerges;
	private Throwable throwable;

	public KWayMergerChunked(CloseSuppressPath workLocation, ExceptionSupplier<S, IOException> chunkSupplier,
			KWayMergerChunkedImpl<E, S> impl, int workers, int k) throws KWayMergerException {
		this(workLocation, chunkSupplier, impl, workers, k, workers);
	}

	/**
	 * K-way merge with a configurable limit on concurrent merge tasks.
	 *
	 * @param workLocation        location to store the chunks
	 * @param chunkSupplier       supplier of per-chunk fluxes
	 * @param impl                implementation that creates/merges chunks
	 * @param workers             number of worker threads
	 * @param k                   merge fan-in
	 * @param maxConcurrentMerges maximum number of merge tasks to run
	 *                            concurrently
	 */
	public KWayMergerChunked(CloseSuppressPath workLocation, ExceptionSupplier<S, IOException> chunkSupplier,
			KWayMergerChunkedImpl<E, S> impl, int workers, int k, int maxConcurrentMerges) throws KWayMergerException {
		this.workLocation = workLocation;
		this.chunkSupplier = chunkSupplier;
		this.impl = impl;
		this.k = k;
		this.maxConcurrentMerges = Math.max(1, maxConcurrentMerges);

		try {
			workLocation.mkdirs();
		} catch (IOException e) {
			throw new KWayMergerException("Can't create workLocation directory!", e);
		}

		this.workers = new Worker[workers];
		int id = ID.incrementAndGet();
		for (int i = 0; i < workers; i++) {
			this.workers[i] = new Worker("KWayMergerChunked#" + id + "Worker#" + i, this);
		}
	}

	public void start() {
		if (started) {
			throw new IllegalArgumentException("The KWayMergerChunked was already started and can't be reused!");
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

	public Optional<CloseSuppressPath> waitResult() throws InterruptedException, KWayMergerException {
		if (!started) {
			throw new IllegalArgumentException("The KWayMergerChunked hasn't been started!");
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

	private CloseSuppressPath getPath() {
		return workLocation.resolve("f-" + pathId.incrementAndGet());
	}

	private KWayMergerRunnable getTask() {
		dataLock.lock();
		try {
			while (true) {
				if (end) {
					if (chunks.size() <= 1) {
						return null;
					}
					if (activeMerges < maxConcurrentMerges) {
						List<Chunk> all = chunks.getAll(k);
						activeMerges++;
						return new MergeTask(all);
					}
					try {
						taskAvailable.await();
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						return null;
					}
					continue;
				}

				if (activeMerges < maxConcurrentMerges) {
					List<Chunk> chunkList = chunks.getMax(k);
					if (chunkList != null) {
						activeMerges++;
						return new MergeTask(chunkList);
					}
				}

				return new GetTask();
			}
		} finally {
			dataLock.unlock();
		}
	}

	private class MergeTask implements KWayMergerRunnable {
		private final List<Chunk> chunks;

		private MergeTask(List<Chunk> chunks) {
			this.chunks = chunks;
		}

		@Override
		public void run() throws KWayMergerException {
			try {
				int height = chunks.stream().mapToInt(Chunk::getHeight).max().orElseThrow() + 1;
				CloseSuppressPath mergeOut = getPath();

				List<CloseSuppressPath> paths = chunks.stream().map(Chunk::getPath)
						.collect(Collectors.toUnmodifiableList());

				KWayMergerException mergeException = null;
				RuntimeException runtimeException = null;
				Error error = null;
				try {
					impl.mergeChunks(paths, mergeOut);
				} catch (KWayMergerException e) {
					mergeException = e;
					throw e;
				} catch (RuntimeException e) {
					runtimeException = e;
					throw e;
				} catch (Error e) {
					error = e;
					throw e;
				} finally {
					try {
						IOUtil.closeAll(paths);
					} catch (IOException closeException) {
						if (mergeException != null) {
							mergeException.addSuppressed(closeException);
						} else if (runtimeException != null) {
							runtimeException.addSuppressed(closeException);
						} else if (error != null) {
							error.addSuppressed(closeException);
						} else {
							throw new KWayMergerException("Can't close end merge files", closeException);
						}
					}
				}

				dataLock.lock();
				try {
					KWayMergerChunked.this.chunks.addElement(new Chunk(height, mergeOut), height);
					taskAvailable.signalAll();
				} finally {
					dataLock.unlock();
				}
			} finally {
				dataLock.lock();
				try {
					activeMerges--;
					taskAvailable.signalAll();
				} finally {
					dataLock.unlock();
				}
			}
		}
	}

	private class GetTask implements KWayMergerRunnable {
		@Override
		public void run() throws KWayMergerException {
			final S flux;
			try {
				flux = chunkSupplier.get();
			} catch (IOException e) {
				throw new KWayMergerException(e);
			}

			if (flux == null) {
				dataLock.lock();
				try {
					end = true;
					taskAvailable.signalAll();
				} finally {
					dataLock.unlock();
				}
				return;
			}

			CloseSuppressPath out = getPath();
			impl.createChunk(flux, out);

			dataLock.lock();
			try {
				Chunk newChunk = new Chunk(1, out);
				chunks.addElement(newChunk, newChunk.getHeight());
				taskAvailable.signalAll();
			} finally {
				dataLock.unlock();
			}
		}
	}

	private static class Chunk {
		private final int height;
		private final CloseSuppressPath path;

		private Chunk(int height, CloseSuppressPath path) {
			this.height = height;
			this.path = path;
		}

		int getHeight() {
			return height;
		}

		CloseSuppressPath getPath() {
			return path;
		}
	}

	private static class Worker extends ExceptionThread {
		private final KWayMergerChunked<?, ?> parent;

		private Worker(String name, KWayMergerChunked<?, ?> parent) {
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

	public interface KWayMergerChunkedImpl<E, S extends Supplier<E>> {
		void createChunk(S flux, CloseSuppressPath output) throws KWayMergerException;

		/**
		 * Merge chunks together into a new chunk.
		 * <p>
		 * Note: the merger owns the lifecycle of {@code inputs}.
		 * Implementations must not close/delete the input paths.
		 */
		void mergeChunks(List<CloseSuppressPath> inputs, CloseSuppressPath output) throws KWayMergerException;
	}
}
