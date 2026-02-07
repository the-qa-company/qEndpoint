package com.the_qa_company.qendpoint.core.util.concurrent;

import com.the_qa_company.qendpoint.core.iterator.utils.AsyncIteratorFetcher;
import com.the_qa_company.qendpoint.core.iterator.utils.SizeFetcher;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KWayMergerMergeConcurrencyTest {
	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void kWayMergerRespectsMergeConcurrencyLimit() throws Exception {
		try (CloseSuppressPath root = CloseSuppressPath.of(tempDir.newFolder().toPath())) {
			root.closeWithDeleteRecurse();

			AtomicInteger activeMerges = new AtomicInteger();
			AtomicInteger maxActiveMerges = new AtomicInteger();
			AtomicInteger mergeCalls = new AtomicInteger();

			KWayMerger.KWayMergerImpl<Integer, Supplier<Integer>> impl = new KWayMerger.KWayMergerImpl<>() {
				@Override
				public void createChunk(Supplier<Integer> flux, CloseSuppressPath output)
						throws KWayMerger.KWayMergerException {
					while (flux.get() != null) {
						// consume
					}
					try (OutputStream os = output.openOutputStream(128)) {
						os.write(1);
					} catch (Exception e) {
						throw new KWayMerger.KWayMergerException(e);
					}
				}

				@Override
				public void mergeChunks(java.util.List<CloseSuppressPath> inputs, CloseSuppressPath output)
						throws KWayMerger.KWayMergerException {
					int active = activeMerges.incrementAndGet();
					maxActiveMerges.accumulateAndGet(active, Math::max);
					mergeCalls.incrementAndGet();
					try {
						LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
						try (OutputStream os = output.openOutputStream(128)) {
							os.write(2);
						}
					} catch (Exception e) {
						throw new KWayMerger.KWayMergerException(e);
					} finally {
						activeMerges.decrementAndGet();
					}
				}

				@Override
				public Supplier<Integer> newStopFlux(Supplier<Integer> flux) {
					return new SizeFetcher<>(flux, ignored -> 1, 1);
				}
			};

			AsyncIteratorFetcher<Integer> fetcher = new AsyncIteratorFetcher<>(
					IntStream.rangeClosed(1, 256).boxed().iterator());

			KWayMerger<Integer, Supplier<Integer>> merger = newMerger(root, fetcher, impl, 4, 2, 1);
			merger.start();
			Optional<CloseSuppressPath> result = merger.waitResult();

			assertFalse(result.isEmpty());
			assertTrue("expected at least one merge", mergeCalls.get() > 0);
			assertEquals("expected merges to run sequentially", 1, maxActiveMerges.get());
		}
	}

	@Test
	public void kWayMergerChunkedRespectsMergeConcurrencyLimit() throws Exception {
		try (CloseSuppressPath root = CloseSuppressPath.of(tempDir.newFolder().toPath())) {
			root.closeWithDeleteRecurse();

			AtomicInteger activeMerges = new AtomicInteger();
			AtomicInteger maxActiveMerges = new AtomicInteger();
			AtomicInteger mergeCalls = new AtomicInteger();
			AtomicInteger nextChunk = new AtomicInteger(1);

			ExceptionSupplier<Supplier<Integer>, java.io.IOException> chunkSupplier = () -> {
				int value = nextChunk.getAndIncrement();
				if (value > 256) {
					return null;
				}
				return new Supplier<>() {
					boolean returned;

					@Override
					public Integer get() {
						if (returned) {
							return null;
						}
						returned = true;
						return value;
					}
				};
			};

			KWayMergerChunked.KWayMergerChunkedImpl<Integer, Supplier<Integer>> impl = new KWayMergerChunked.KWayMergerChunkedImpl<>() {
				@Override
				public void createChunk(Supplier<Integer> flux, CloseSuppressPath output)
						throws KWayMerger.KWayMergerException {
					while (flux.get() != null) {
						// consume
					}
					try (OutputStream os = output.openOutputStream(128)) {
						os.write(1);
					} catch (Exception e) {
						throw new KWayMerger.KWayMergerException(e);
					}
				}

				@Override
				public void mergeChunks(java.util.List<CloseSuppressPath> inputs, CloseSuppressPath output)
						throws KWayMerger.KWayMergerException {
					int active = activeMerges.incrementAndGet();
					maxActiveMerges.accumulateAndGet(active, Math::max);
					mergeCalls.incrementAndGet();
					try {
						LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
						try (OutputStream os = output.openOutputStream(128)) {
							os.write(2);
						}
					} catch (Exception e) {
						throw new KWayMerger.KWayMergerException(e);
					} finally {
						activeMerges.decrementAndGet();
					}
				}
			};

			KWayMergerChunked<Integer, Supplier<Integer>> merger = newChunkedMerger(root, chunkSupplier, impl, 4, 2, 1);
			merger.start();
			Optional<CloseSuppressPath> result = merger.waitResult();

			assertFalse(result.isEmpty());
			assertTrue("expected at least one merge", mergeCalls.get() > 0);
			assertEquals("expected merges to run sequentially", 1, maxActiveMerges.get());
		}
	}

	@SuppressWarnings("unchecked")
	private static <E, S extends Supplier<E>> KWayMerger<E, S> newMerger(CloseSuppressPath workLocation,
			AsyncIteratorFetcher<E> fetcher, KWayMerger.KWayMergerImpl<E, S> impl, int workers, int k,
			int maxConcurrentMerges) throws Exception {
		Constructor<KWayMerger> ctor = KWayMerger.class.getConstructor(CloseSuppressPath.class,
				AsyncIteratorFetcher.class, KWayMerger.KWayMergerImpl.class, int.class, int.class, int.class);
		return (KWayMerger<E, S>) ctor.newInstance(workLocation, fetcher, impl, workers, k, maxConcurrentMerges);
	}

	@SuppressWarnings("unchecked")
	private static <E, S extends Supplier<E>> KWayMergerChunked<E, S> newChunkedMerger(CloseSuppressPath workLocation,
			ExceptionSupplier<S, java.io.IOException> chunkSupplier, KWayMergerChunked.KWayMergerChunkedImpl<E, S> impl,
			int workers, int k, int maxConcurrentMerges) throws Exception {
		Constructor<KWayMergerChunked> ctor = KWayMergerChunked.class.getConstructor(CloseSuppressPath.class,
				ExceptionSupplier.class, KWayMergerChunked.KWayMergerChunkedImpl.class, int.class, int.class,
				int.class);
		return (KWayMergerChunked<E, S>) ctor.newInstance(workLocation, chunkSupplier, impl, workers, k,
				maxConcurrentMerges);
	}
}
