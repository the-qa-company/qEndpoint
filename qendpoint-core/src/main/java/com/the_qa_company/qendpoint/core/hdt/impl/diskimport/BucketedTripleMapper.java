package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.dictionary.impl.CompressFourSectionDictionary;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.Lz4Config;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bucketed mapping sink for disk imports.
 * <p>
 * Instead of writing tripleId-indexed mapping arrays with random writes during
 * dictionary construction, this implementation appends (offsetInBucket,
 * headerId) records into per-bucket files. After dictionary construction,
 * {@link #materializeTo(CompressTripleMapper)} can be used to populate the
 * standard {@link CompressTripleMapper} mapping files sequentially.
 */
public class BucketedTripleMapper implements CompressFourSectionDictionary.NodeConsumer, Closeable {
	private static final Logger log = LoggerFactory.getLogger(BucketedTripleMapper.class);
	private static final int RECORD_BYTES = Integer.BYTES + Long.BYTES;
	private static volatile MaterializationObserver materializationObserver;

	interface MaterializationObserver {
		void onMaterialization(String role, String threadName);
	}

	static void setMaterializationObserver(MaterializationObserver observer) {
		materializationObserver = observer;
	}

	private final RoleSpooler subjects;
	private final RoleSpooler predicates;
	private final RoleSpooler objects;
	private final RoleSpooler graphs;
	private final boolean supportsGraph;

	public BucketedTripleMapper(CloseSuppressPath location, long tripleCount, boolean supportsGraph, int bucketSize,
			int bufferSize) throws IOException {
		this.supportsGraph = supportsGraph;
		location.mkdirs();
		subjects = new RoleSpooler(location.resolve("subjects"), tripleCount, bucketSize, bufferSize);
		predicates = new RoleSpooler(location.resolve("predicates"), tripleCount, bucketSize, bufferSize);
		objects = new RoleSpooler(location.resolve("objects"), tripleCount, bucketSize, bufferSize);
		if (supportsGraph) {
			graphs = new RoleSpooler(location.resolve("graphs"), tripleCount, bucketSize, bufferSize);
		} else {
			graphs = null;
		}
	}

	@Override
	public void onSubject(long preMapId, long newMapId) {
		subjects.add(preMapId, newMapId);
	}

	@Override
	public void onSubject(long[] preMapIds, long[] newMapIds, int offset, int length) {
		subjects.add(preMapIds, newMapIds, offset, length);
	}

	@Override
	public void onPredicate(long preMapId, long newMapId) {
		predicates.add(preMapId, newMapId);
	}

	@Override
	public void onPredicate(long[] preMapIds, long[] newMapIds, int offset, int length) {
		predicates.add(preMapIds, newMapIds, offset, length);
	}

	@Override
	public void onObject(long preMapId, long newMapId) {
		objects.add(preMapId, newMapId);
	}

	@Override
	public void onObject(long[] preMapIds, long[] newMapIds, int offset, int length) {
		objects.add(preMapIds, newMapIds, offset, length);
	}

	@Override
	public void onGraph(long preMapId, long newMapId) {
		if (!supportsGraph) {
			return;
		}
		graphs.add(preMapId, newMapId);
	}

	@Override
	public void onGraph(long[] preMapIds, long[] newMapIds, int offset, int length) {
		if (!supportsGraph) {
			return;
		}
		graphs.add(preMapIds, newMapIds, offset, length);
	}

	public void materializeTo(CompressTripleMapper mapper) throws IOException {
		materializeTo(mapper, ProgressListener.ignore());
	}

	public void materializeTo(CompressTripleMapper mapper, ProgressListener listener) throws IOException {
		flush();
		ProgressListener progressListener = ProgressListener.ofNullable(listener);
		List<CompletableFuture<Void>> futures = new ArrayList<>();
		int roleCount = supportsGraph ? 4 : 3;
		AtomicInteger threadId = new AtomicInteger(1);
		ExecutorService executor = Executors.newFixedThreadPool(roleCount, runnable -> {
			Thread thread = new Thread(runnable, "BucketedTripleMapper-materialize-" + threadId.getAndIncrement());
			thread.setDaemon(true);
			return thread;
		});

		try {
			futures.add(CompletableFuture.runAsync(() -> {
				try {
					subjects.materialize(Role.SUBJECT, mapper, progressListener);
				} catch (IOException e) {
					throw new CompletionException(e);
				}
			}, executor));
			futures.add(CompletableFuture.runAsync(() -> {
				try {
					predicates.materialize(Role.PREDICATE, mapper, progressListener);
				} catch (IOException e) {
					throw new CompletionException(e);
				}
			}, executor));
			futures.add(CompletableFuture.runAsync(() -> {
				try {
					objects.materialize(Role.OBJECT, mapper, progressListener);
				} catch (IOException e) {
					throw new CompletionException(e);
				}
			}, executor));
			if (supportsGraph) {
				futures.add(CompletableFuture.runAsync(() -> {
					try {
						graphs.materialize(Role.GRAPH, mapper, progressListener);
					} catch (IOException e) {
						throw new CompletionException(e);
					}
				}, executor));
			}

			CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
		} catch (CompletionException e) {
			for (CompletableFuture<Void> future : futures) {
				future.cancel(true);
			}
			Throwable cause = e.getCause();
			if (cause instanceof IOException io) {
				throw io;
			}
			throw e;
		} catch (Throwable t) {
			for (CompletableFuture<Void> future : futures) {
				future.cancel(true);
			}
			throw t;
		} finally {
			executor.shutdown();
			if (!executor.isTerminated()) {
				executor.shutdownNow();
				try {
					executor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}

				if (!executor.isTerminated()) {
					log.warn("Executor did not terminate after shutdownNow");
				}
			}
		}
	}

	private void flush() throws IOException {
		subjects.flush();
		predicates.flush();
		objects.flush();
		if (supportsGraph) {
			graphs.flush();
		}
	}

	@Override
	public void close() throws IOException {
		flush();
		subjects.close();
		predicates.close();
		objects.close();
		if (supportsGraph) {
			graphs.close();
		}
	}

	private enum Role {
		SUBJECT, PREDICATE, OBJECT, GRAPH
	}

	private static void notifyMaterialization(Role role) {
		MaterializationObserver observer = materializationObserver;
		if (observer != null) {
			observer.onMaterialization(role.name(), Thread.currentThread().getName());
		}
	}

	private static final class RoleSpooler implements Closeable {
		private static final int IO_BUFFER_BYTES = 1024 * 1024;
		private static final int CHUNK_HEADER_BYTES = Integer.BYTES + Integer.BYTES;

		// --- Async I/O tuning knobs ---
		// Bounds simultaneously-open bucket files per flush batch (avoid FD
		// explosion).
		private static final int MAX_IN_FLIGHT_BUCKETS = 256;

		// LRU of idle channels kept open across flushes.
		private static final int MAX_CACHED_CHANNELS = 512;

		/*
		 * ByteBuffer pool knobs (heap buffers with backing arrays)
		 */

		// round capacities to 4KiB
		private static final int WRITE_BUF_ROUNDING = 4 * 1024;

		// total pooled memory cap
		private static final int WRITE_BUF_POOL_MAX_BYTES = 256 * 1024 * 1024;

		// per-size cap
		private static final int WRITE_BUF_POOL_MAX_PER_SIZE = 8;

		// don't pool buffers > 2MiB
		private static final int WRITE_BUF_POOL_MAX_BUFFER_BYTES = 16 * 1024 * 1024;

		private final CloseSuppressPath root;
		private final long tripleCount;
		private final int bucketSize;
		private final int bucketCount;

		private final int[] bucketBuffer;
		private final int[] offsetBuffer;
		private final long[] headerBuffer;
		private int size;

		private final int[] bucketCounts;
		private final int[] bucketOffsets;
		private final int[] bucketWriteCursor;
		private final int[] sortedOffsets;
		private final long[] sortedHeaders;

		// Used for packing uncompressed records before compression
		private final byte[] ioBuffer;

		// Used only by readRecords() path
		private final byte[] compressedBuffer;
		private final byte[] chunkHeader;

		private final LZ4Compressor compressor;
		private final LZ4SafeDecompressor decompressor;

		private final ChannelCache channelCache;
		private final ByteBufferPool writeBufferPool;
		private final boolean compressionEnabled;

		// Reused per-flush lists to reduce allocation churn
		private final ArrayList<BucketChannel> inFlightBuckets = new ArrayList<>(MAX_IN_FLIGHT_BUCKETS);
		private final ArrayList<CompletableFuture<Void>> inFlightWrites = new ArrayList<>(MAX_IN_FLIGHT_BUCKETS * 2);

		private RoleSpooler(CloseSuppressPath root, long tripleCount, int bucketSize, int bufferSize)
				throws IOException {
			if (tripleCount < 0) {
				throw new IllegalArgumentException("Negative tripleCount: " + tripleCount);
			}
			if (bucketSize <= 0) {
				throw new IllegalArgumentException("bucketSize must be > 0");
			}
			if (bufferSize <= 0) {
				throw new IllegalArgumentException("bufferSize must be > 0");
			}
			this.root = root;
			this.tripleCount = tripleCount;
			this.bucketSize = bucketSize;
			this.bucketCount = (int) Math.max(1, (tripleCount + bucketSize - 1) / bucketSize);

			root.mkdirs();

			bucketBuffer = new int[bufferSize];
			offsetBuffer = new int[bufferSize];
			headerBuffer = new long[bufferSize];

			bucketCounts = new int[bucketCount];
			bucketOffsets = new int[bucketCount + 1];
			bucketWriteCursor = new int[bucketCount];
			sortedOffsets = new int[bufferSize];
			sortedHeaders = new long[bufferSize];

			LZ4Factory factory = LZ4Factory.fastestInstance();
			compressor = factory.fastCompressor();
			decompressor = factory.safeDecompressor();

			ioBuffer = new byte[IO_BUFFER_BYTES];
			compressedBuffer = new byte[compressor.maxCompressedLength(IO_BUFFER_BYTES)];
			chunkHeader = new byte[CHUNK_HEADER_BYTES];

			channelCache = new ChannelCache(Math.min(MAX_CACHED_CHANNELS, bucketCount));
			writeBufferPool = new ByteBufferPool(WRITE_BUF_ROUNDING, WRITE_BUF_POOL_MAX_BYTES,
					WRITE_BUF_POOL_MAX_PER_SIZE, WRITE_BUF_POOL_MAX_BUFFER_BYTES);
			compressionEnabled = Lz4Config.ENABLED;
		}

		private void add(long tripleId, long headerId) {
			if (tripleId <= 0) {
				throw new IllegalArgumentException("tripleId must be > 0");
			}
			long value = tripleId - 1;
			int bucket = (int) (value / bucketSize);
			int offset = (int) (value - (long) bucket * bucketSize);
			bucketBuffer[size] = bucket;
			offsetBuffer[size] = offset;
			headerBuffer[size] = headerId;
			size++;
			if (size == bucketBuffer.length) {
				try {
					flush();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}

		private void add(long[] tripleIds, long[] headerIds, int offset, int length) {
			for (int i = offset; i < offset + length; i++) {
				add(tripleIds[i], headerIds[i]);
			}
		}

		/**
		 * Async NIO flush: - groups buffered entries by bucket - packs +
		 * compresses records into pooled ByteBuffers - schedules
		 * AsynchronousFileChannel writes for each chunk - waits for all writes
		 * in a bounded batch (avoids opening too many files at once) - caches
		 * idle channels (LRU) to reduce open/close overhead
		 */
		private void flush() throws IOException {
			if (size == 0) {
				return;
			}

			Arrays.fill(bucketCounts, 0);
			for (int i = 0; i < size; i++) {
				bucketCounts[bucketBuffer[i]]++;
			}

			bucketOffsets[0] = 0;
			for (int b = 0; b < bucketCount; b++) {
				bucketOffsets[b + 1] = bucketOffsets[b] + bucketCounts[b];
			}
			System.arraycopy(bucketOffsets, 0, bucketWriteCursor, 0, bucketCount);

			for (int i = 0; i < size; i++) {
				int bucket = bucketBuffer[i];
				int outIndex = bucketWriteCursor[bucket]++;
				sortedOffsets[outIndex] = offsetBuffer[i];
				sortedHeaders[outIndex] = headerBuffer[i];
			}

			inFlightBuckets.clear();
			inFlightWrites.clear();

			try {
				for (int bucket = 0; bucket < bucketCount; bucket++) {
					int start = bucketOffsets[bucket];
					int end = bucketOffsets[bucket + 1];
					if (start == end) {
						continue;
					}

					CloseSuppressPath file = root.resolve(bucketFileName(bucket));
					CachedChannel ch = channelCache.acquire(bucket, file);

					inFlightBuckets.add(new BucketChannel(bucket, ch));
					scheduleBucketWrites(ch, sortedOffsets, sortedHeaders, start, end, inFlightWrites);

					if (inFlightBuckets.size() >= MAX_IN_FLIGHT_BUCKETS) {
						awaitBatchAndRelease(/* cacheOnSuccess= */true);
					}
				}

				awaitBatchAndRelease(/* cacheOnSuccess= */true);

				size = 0;
			} catch (IOException e) {
				// Best-effort cleanup: drain/close in-flight channels without
				// caching positions.
				try {
					awaitBatchAndRelease(/* cacheOnSuccess= */false);
				} catch (IOException cleanup) {
					e.addSuppressed(cleanup);
				}
				throw e;
			}
		}

		private void scheduleBucketWrites(CachedChannel channel, int[] offsets, long[] headers, int from, int to,
				List<CompletableFuture<Void>> writes) throws IOException {

			byte[] uncompressed = ioBuffer;
			int pos = 0;

			for (int i = from; i < to; i++) {
				if (uncompressed.length - pos < RECORD_BYTES) {
					writes.add(scheduleChunkWrite(channel, uncompressed, pos));
					pos = 0;
				}

				putInt(uncompressed, pos, offsets[i]);
				pos += Integer.BYTES;

				putLong(uncompressed, pos, headers[i]);
				pos += Long.BYTES;
			}

			if (pos > 0) {
				writes.add(scheduleChunkWrite(channel, uncompressed, pos));
			}
		}

		private CompletableFuture<Void> scheduleChunkWrite(CachedChannel channel, byte[] uncompressed, int length)
				throws IOException {
			if (length <= 0) {
				return CompletableFuture.completedFuture(null);
			}

			int payloadCapacity = compressionEnabled ? compressor.maxCompressedLength(length) : length;
			int minCapacity = CHUNK_HEADER_BYTES + payloadCapacity;

			ByteBuffer out = writeBufferPool.acquire(minCapacity);
			if (!out.hasArray()) {
				// With our pool this should not happen, but be defensive.
				writeBufferPool.release(out);
				throw new IOException("Write buffer must be a heap ByteBuffer with accessible array");
			}

			byte[] outArray = out.array();
			int base = out.arrayOffset(); // typically 0

			int compressedLength;
			int payloadLength;
			if (compressionEnabled) {
				compressedLength = compressor.compress(uncompressed, 0, length, outArray, base + CHUNK_HEADER_BYTES,
						outArray.length - (base + CHUNK_HEADER_BYTES));
				payloadLength = compressedLength;
			} else {
				System.arraycopy(uncompressed, 0, outArray, base + CHUNK_HEADER_BYTES, length);
				compressedLength = -length;
				payloadLength = length;
			}

			// header (big-endian ints, matches existing on-disk format)
			putInt(outArray, base, length);
			putInt(outArray, base + Integer.BYTES, compressedLength);

			int totalLen = CHUNK_HEADER_BYTES + payloadLength;

			if (totalLen > out.capacity()) {
				log.error(
						"BucketedTripleMapper chunk limit > capacity: totalLen={} capacity={} minCapacity={} "
								+ "payloadCapacity={} payloadLength={} compressionEnabled={} length={}",
						totalLen, out.capacity(), minCapacity, payloadCapacity, payloadLength, compressionEnabled,
						length);
			}

			out.clear();
			out.limit(totalLen);

			long writePos = channel.reserve(totalLen);

			CompletableFuture<Void> f = writeFully(channel.channel, out, writePos);
			return f.whenComplete((ok, err) -> writeBufferPool.release(out));
		}

		private void awaitBatchAndRelease(boolean cacheOnSuccess) throws IOException {
			if (inFlightBuckets.isEmpty()) {
				inFlightWrites.clear();
				return;
			}

			Throwable failure = null;

			if (!inFlightWrites.isEmpty()) {
				CompletableFuture<Void> all = CompletableFuture.allOf(inFlightWrites.toArray(new CompletableFuture[0]));
				try {
					all.join();
				} catch (CompletionException e) {
					failure = (e.getCause() != null) ? e.getCause() : e;
				}
			}

			if (failure == null && cacheOnSuccess) {
				for (BucketChannel bc : inFlightBuckets) {
					channelCache.release(bc.bucket, bc.channel);
				}
			} else {
				for (BucketChannel bc : inFlightBuckets) {
					closeQuietly(bc.channel.channel);
				}
			}

			inFlightBuckets.clear();
			inFlightWrites.clear();

			if (failure != null) {
				throw toIOException(failure);
			}
		}

		private static IOException toIOException(Throwable t) {
			if (t instanceof IOException ioe) {
				return ioe;
			}
			return new IOException("Async bucket write failed", t);
		}

		private static void closeQuietly(AsynchronousFileChannel ch) {
			if (ch == null) {
				return;
			}
			try {
				ch.close();
			} catch (IOException ignored) {
			}
		}

		private static CompletableFuture<Void> writeFully(AsynchronousFileChannel ch, ByteBuffer buf, long position) {
			CompletableFuture<Void> done = new CompletableFuture<>();
			writeFully0(ch, buf, position, done);
			return done;
		}

		private static void writeFully0(AsynchronousFileChannel ch, ByteBuffer buf, long position,
				CompletableFuture<Void> done) {
			ch.write(buf, position, done, new CompletionHandler<>() {
				@Override
				public void completed(Integer written, CompletableFuture<Void> promise) {
					if (written == null || written < 0) {
						promise.completeExceptionally(new EOFException("AsyncFileChannel.write returned " + written));
						return;
					}
					long nextPos = position + written;
					if (buf.hasRemaining()) {
						// Continue writing remaining bytes
						writeFully0(ch, buf, nextPos, promise);
					} else {
						promise.complete(null);
					}
				}

				@Override
				public void failed(Throwable exc, CompletableFuture<Void> promise) {
					promise.completeExceptionally(exc);
				}
			});
		}

		private static final class BucketChannel {
			final int bucket;
			final CachedChannel channel;

			BucketChannel(int bucket, CachedChannel channel) {
				this.bucket = bucket;
				this.channel = channel;
			}
		}

		private static final class CachedChannel {
			final AsynchronousFileChannel channel;
			long position;

			CachedChannel(AsynchronousFileChannel channel, long position) {
				this.channel = channel;
				this.position = position;
			}

			long reserve(int bytes) {
				long p = position;
				position += bytes;
				return p;
			}
		}

		/**
		 * LRU cache of *idle* channels keyed by bucket id (int). A channel is
		 * removed from the cache when acquired, and returned on release.
		 */
		private static final class ChannelCache implements Closeable {
			private final int maxCached;
			private final LinkedHashMap<Integer, CachedChannel> lru;

			ChannelCache(int maxCached) {
				this.maxCached = Math.max(0, maxCached);
				this.lru = new LinkedHashMap<>(16, 0.75f, true);
			}

			CachedChannel acquire(int bucket, CloseSuppressPath file) throws IOException {
				Integer key = bucket;
				CachedChannel cached = lru.remove(key);
				if (cached != null && cached.channel.isOpen()) {
					return cached;
				}
				closeQuietly(cached);

				AsynchronousFileChannel ch = AsynchronousFileChannel.open(file, StandardOpenOption.CREATE,
						StandardOpenOption.WRITE);
				long pos = ch.size(); // append position
				return new CachedChannel(ch, pos);
			}

			void release(int bucket, CachedChannel ch) {
				if (ch == null || ch.channel == null || !ch.channel.isOpen() || maxCached == 0) {
					closeQuietly(ch);
					return;
				}

				lru.put(bucket, ch);

				while (lru.size() > maxCached) {
					Iterator<Map.Entry<Integer, CachedChannel>> it = lru.entrySet().iterator();
					Map.Entry<Integer, CachedChannel> eldest = it.next();
					it.remove();
					closeQuietly(eldest.getValue());
				}
			}

			@Override
			public void close() throws IOException {
				IOException first = null;
				for (CachedChannel ch : lru.values()) {
					try {
						if (ch != null && ch.channel != null) {
							ch.channel.close();
						}
					} catch (IOException e) {
						if (first == null) {
							first = e;
						} else {
							first.addSuppressed(e);
						}
					}
				}
				lru.clear();
				if (first != null) {
					throw first;
				}
			}

			private static void closeQuietly(CachedChannel ch) {
				if (ch == null || ch.channel == null) {
					return;
				}
				try {
					ch.channel.close();
				} catch (IOException ignored) {
				}
			}
		}

		/**
		 * Pool of heap ByteBuffers by rounded capacity. Keeps compression
		 * output buffers reusable without unbounded memory growth.
		 */
		private static final class ByteBufferPool {
			private final int rounding;
			private final int maxTotalBytes;
			private final int maxPerSize;
			private final int maxBufferBytes;

			private final Object lock = new Object();
			private final Map<Integer, ArrayDeque<ByteBuffer>> bySize = new HashMap<>();
			private int totalBytes;

			ByteBufferPool(int rounding, int maxTotalBytes, int maxPerSize, int maxBufferBytes) {
				this.rounding = Math.max(1, rounding);
				this.maxTotalBytes = Math.max(0, maxTotalBytes);
				this.maxPerSize = Math.max(0, maxPerSize);
				this.maxBufferBytes = Math.max(0, maxBufferBytes);
			}

			ByteBuffer acquire(int minCapacity) {
				int cap = roundUp(minCapacity, rounding);
				if (cap <= 0) {
					cap = minCapacity;
				}

				synchronized (lock) {
					ArrayDeque<ByteBuffer> q = bySize.get(cap);
					if (q != null) {
						ByteBuffer buf = q.pollFirst();
						if (buf != null) {
							totalBytes -= cap;
							buf.clear();
							return buf;
						}
					}
				}

				// Heap buffer so we can compress directly into its backing
				// byte[]
				return ByteBuffer.allocate(cap);
			}

			void release(ByteBuffer buf) {
				if (buf == null) {
					return;
				}

				int cap = buf.capacity();
				if (cap <= 0) {
					return;
				}
				if (cap > maxBufferBytes) {
					return; // don't pool huge buffers
				}

				buf.clear();

				synchronized (lock) {
					if (totalBytes + cap > maxTotalBytes) {
						return;
					}
					ArrayDeque<ByteBuffer> q = bySize.computeIfAbsent(cap, k -> new ArrayDeque<>());
					if (q.size() >= maxPerSize) {
						return;
					}
					q.addFirst(buf);
					totalBytes += cap;
				}
			}

			void clear() {
				synchronized (lock) {
					bySize.clear();
					totalBytes = 0;
				}
			}

			private static int roundUp(int x, int multiple) {
				int m = multiple;
				int r = x % m;
				return r == 0 ? x : (x + (m - r));
			}
		}

		private void materialize(Role role, CompressTripleMapper mapper, ProgressListener listener) throws IOException {
			ProgressListener progressListener = ProgressListener.ofNullable(listener);
			notifyMaterialization(role);
			if (tripleCount == 0) {
				progressListener.notifyProgress(100, "Materialized " + role + " mapping");
				return;
			}
			progressListener.notifyProgress(0, "Materializing " + role + " mapping");
			long processed = 0;
			int lastPercent = 0;
			for (int bucket = 0; bucket < bucketCount; bucket++) {
				if (Thread.currentThread().isInterrupted()) {
					throw new RuntimeException("Materialization interrupted");
				}

				long bucketStart = (long) bucket * bucketSize + 1;
				long remaining = tripleCount - (bucketStart - 1);
				if (remaining <= 0) {
					break;
				}
				int count = (int) Math.min(bucketSize, remaining);

				long[] headers = new long[count];
				CloseSuppressPath file = root.resolve(bucketFileName(bucket));
				if (!Files.exists(file)) {
					throw new IllegalStateException("Missing bucket file: " + file);
				}
				try (InputStream in = new BufferedInputStream(Files.newInputStream(file), IO_BUFFER_BYTES)) {
					readRecords(in, headers);
				}

				for (int i = 0; i < count; i++) {
					long tripleId = bucketStart + i;
					long headerId = headers[i];
					if (headerId == 0) {
						throw new IllegalStateException("Missing mapping for tripleId=" + tripleId + " (" + role + ")");
					}
					switch (role) {
					case SUBJECT -> mapper.onSubject(tripleId, headerId);
					case PREDICATE -> mapper.onPredicate(tripleId, headerId);
					case OBJECT -> mapper.onObject(tripleId, headerId);
					case GRAPH -> mapper.onGraph(tripleId, headerId);
					default -> throw new IllegalStateException("Unknown role: " + role);
					}
				}
				processed += count;
				int percent = (int) ((processed * 100d) / tripleCount);
				if (percent > lastPercent) {
					progressListener.notifyProgress(percent,
							"Materializing " + role + " mapping " + processed + "/" + tripleCount);
					lastPercent = percent;
				}
			}
			progressListener.notifyProgress(100, "Materialized " + role + " mapping");
		}

		private void readRecords(InputStream in, long[] headersByOffset) throws IOException {
			byte[] header = chunkHeader;
			byte[] buffer = ioBuffer;
			byte[] compressed = compressedBuffer;
			while (true) {
				int headerRead = readFully(in, header, 0, CHUNK_HEADER_BYTES);
				if (headerRead == -1) {
					return;
				}
				if (headerRead < CHUNK_HEADER_BYTES) {
					throw new EOFException(
							"Unexpected end of chunk header: " + headerRead + " < " + CHUNK_HEADER_BYTES);
				}
				int uncompressedLength = getInt(header, 0);
				int compressedLength = getInt(header, Integer.BYTES);
				int payloadLength = compressedLength > 0 ? compressedLength : -compressedLength;
				if (uncompressedLength <= 0 || payloadLength <= 0) {
					throw new EOFException("Unexpected chunk lengths: uncompressed=" + uncompressedLength
							+ " compressed=" + compressedLength);
				}
				if (uncompressedLength > buffer.length) {
					throw new IOException("Chunk too large: " + uncompressedLength + " > " + buffer.length);
				}
				if (compressedLength > 0 && compressedLength > compressed.length) {
					throw new IOException(
							"Compressed chunk too large: " + compressedLength + " > " + compressed.length);
				}
				if (compressedLength < 0 && payloadLength != uncompressedLength) {
					throw new EOFException(
							"Unexpected raw chunk length: " + payloadLength + " != " + uncompressedLength);
				}
				int read;
				if (compressedLength > 0) {
					read = readFully(in, compressed, 0, payloadLength);
					if (read < payloadLength) {
						throw new EOFException("Unexpected end of compressed chunk: " + read + " < " + payloadLength);
					}
				} else {
					read = readFully(in, buffer, 0, payloadLength);
					if (read < payloadLength) {
						throw new EOFException("Unexpected end of raw chunk: " + read + " < " + payloadLength);
					}
				}
				if (uncompressedLength % RECORD_BYTES != 0) {
					throw new EOFException("Unexpected uncompressed chunk length: " + uncompressedLength);
				}
				if (compressedLength > 0) {
					try {
						int decompressed = decompressor.decompress(compressed, 0, payloadLength, buffer, 0,
								uncompressedLength);
						if (decompressed != uncompressedLength) {
							throw new IOException(
									"Unexpected decompressed length: " + decompressed + " != " + uncompressedLength);
						}
					} catch (LZ4Exception e) {
						throw new IOException("Corrupt LZ4 chunk", e);
					}
				}
				for (int pos = 0; pos < uncompressedLength; pos += RECORD_BYTES) {
					int offset = getInt(buffer, pos);
					long headerId = getLong(buffer, pos + Integer.BYTES);
					if (offset < 0 || offset >= headersByOffset.length) {
						throw new IOException("Offset out of range: " + offset + " >= " + headersByOffset.length);
					}
					headersByOffset[offset] = headerId;
				}
			}
		}

		private static int readFully(InputStream in, byte[] buffer, int offset, int length) throws IOException {
			int total = 0;
			while (total < length) {
				int read = in.read(buffer, offset + total, length - total);
				if (read == -1) {
					return total == 0 ? -1 : total;
				}
				total += read;
			}
			return total;
		}

		private static void putInt(byte[] buffer, int offset, int value) {
			buffer[offset] = (byte) (value >>> 24);
			buffer[offset + 1] = (byte) (value >>> 16);
			buffer[offset + 2] = (byte) (value >>> 8);
			buffer[offset + 3] = (byte) value;
		}

		private static void putLong(byte[] buffer, int offset, long value) {
			buffer[offset] = (byte) (value >>> 56);
			buffer[offset + 1] = (byte) (value >>> 48);
			buffer[offset + 2] = (byte) (value >>> 40);
			buffer[offset + 3] = (byte) (value >>> 32);
			buffer[offset + 4] = (byte) (value >>> 24);
			buffer[offset + 5] = (byte) (value >>> 16);
			buffer[offset + 6] = (byte) (value >>> 8);
			buffer[offset + 7] = (byte) value;
		}

		private static int getInt(byte[] buffer, int offset) {
			return ((buffer[offset] & 0xff) << 24) | ((buffer[offset + 1] & 0xff) << 16)
					| ((buffer[offset + 2] & 0xff) << 8) | (buffer[offset + 3] & 0xff);
		}

		private static long getLong(byte[] buffer, int offset) {
			return ((long) (buffer[offset] & 0xff) << 56) | ((long) (buffer[offset + 1] & 0xff) << 48)
					| ((long) (buffer[offset + 2] & 0xff) << 40) | ((long) (buffer[offset + 3] & 0xff) << 32)
					| ((long) (buffer[offset + 4] & 0xff) << 24) | ((long) (buffer[offset + 5] & 0xff) << 16)
					| ((long) (buffer[offset + 6] & 0xff) << 8) | ((long) (buffer[offset + 7] & 0xff));
		}

		private static String bucketFileName(int bucket) {
			if (bucket < 0 || bucket >= 1_000_000) {
				// fallback that preserves the original semantics
				return bucketFileNameSlowButGeneral(bucket);
			}

			char[] c = { 'b', '0', '0', '0', '0', '0', '0', '.', 'b', 'i', 'n' };
			int x = bucket;

			c[6] = (char) ('0' + (x % 10));
			x /= 10;
			c[5] = (char) ('0' + (x % 10));
			x /= 10;
			c[4] = (char) ('0' + (x % 10));
			x /= 10;
			c[3] = (char) ('0' + (x % 10));
			x /= 10;
			c[2] = (char) ('0' + (x % 10));
			x /= 10;
			c[1] = (char) ('0' + x); // now 0..9

			return new String(c);
		}

		private static final String ZEROS_6 = "000000";

		private static String bucketFileNameSlowButGeneral(int bucket) {
			// same implementation as the "Fast and fully correct" version above
			if (bucket >= 0) {
				String digits = Integer.toString(bucket);
				int pad = 6 - digits.length();
				StringBuilder sb = new StringBuilder(1 + Math.max(6, digits.length()) + 4);
				sb.append('b');
				if (pad > 0) {
					sb.append(ZEROS_6, 0, pad);
				}
				sb.append(digits).append(".bin");
				return sb.toString();
			}
			long v = -(long) bucket;
			String digits = Long.toString(v);
			int pad = 5 - digits.length();
			StringBuilder sb = new StringBuilder(1 + 1 + Math.max(5, digits.length()) + 4);
			sb.append('b').append('-');
			if (pad > 0) {
				sb.append(ZEROS_6, 0, pad);
			}
			sb.append(digits).append(".bin");
			return sb.toString();
		}

		@Override
		public void close() throws IOException {
			// Close cached channels (important for Windows delete semantics),
			// then delete directory.
			IOException first = null;

			try {
				channelCache.close();
			} catch (IOException e) {
				first = e;
			} finally {
				writeBufferPool.clear();
			}

			root.closeWithDeleteRecurse();

			try {
				root.close();
			} catch (IOException e) {
				log.warn("Can't delete bucketed mapping directory {}", root, e);
			}

			if (first != null) {
				throw first;
			}
		}
	}
}
