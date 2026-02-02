/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/triples
 * /impl/BucketedSequenceWriter.java $ Revision: $Rev: 203 $ Last modified:
 * $Date: 2013-05-24 10:48:53 +0100 (vie, 24 may 2013) $ Last modified by:
 * $Author: mario.arias $ This library is free software; you can redistribute it
 * and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; version 3.0 of the License. This
 * library is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 * Contacting the authors: Mario Arias: mario.arias@deri.org Javier D.
 * Fernandez: jfergar@infor.uva.es Miguel A. Martinez-Prieto:
 * migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.compact.sequence.DynamicSequence;
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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

final class BucketedSequenceWriter implements Closeable {
	private static final Logger log = LoggerFactory.getLogger(BucketedSequenceWriter.class);
	private static final int OBJECT_INDEX_BUCKET_SIZE = 64 * 1024 * 1024;
	private static final int OBJECT_INDEX_BUFFER_RECORDS = 1024 * 1024;
	private static final int OBJECT_INDEX_IO_BUFFER_BYTES = 1024 * 1024;
	private static final int OBJECT_INDEX_RECORD_BYTES = Integer.BYTES + Long.BYTES;
	private static final int OBJECT_INDEX_CHUNK_HEADER_BYTES = Integer.BYTES + Integer.BYTES;
	private static final long OBJECT_INDEX_MISSING = -1L;
	private static final int PARALLEL_WRITE_MIN_BUCKETS = 2;
	private static final int PARALLEL_WRITE_MIN_RECORDS = 1 << 14;
	private static final int MAX_CACHED_CHANNELS = 512;
	private static final int WRITE_BUF_ROUNDING = 4 * 1024;
	private static final int WRITE_BUF_POOL_MAX_BYTES = 256 * 1024 * 1024;
	private static final int WRITE_BUF_POOL_MAX_PER_SIZE = 8;
	private static final int WRITE_BUF_POOL_MAX_BUFFER_BYTES = 16 * 1024 * 1024;
	private static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();
	private static final LZ4SafeDecompressor LZ4_DECOMPRESSOR = LZ4_FACTORY.safeDecompressor();
	private static final ThreadLocal<WriteBuffers> WRITE_BUFFERS = ThreadLocal.withInitial(WriteBuffers::new);
	private static volatile BucketWriteObserver bucketWriteObserver;

	interface BucketWriteObserver {
		void onBucketWrite(int bucket, String threadName);
	}

	static void setBucketWriteObserver(BucketWriteObserver observer) {
		bucketWriteObserver = observer;
	}

	private final CloseSuppressPath root;
	private final long totalEntries;
	private final int bucketSize;
	private final int bucketCount;

	private final int[] bucketBuffer;
	private final int[] offsetBuffer;
	private final long[] valueBuffer;
	private int size;

	private final int[] bucketCounts;
	private final int[] bucketOffsets;
	private final int[] bucketWriteCursor;
	private final int[] sortedOffsets;
	private final long[] sortedValues;
	private final byte[] ioBuffer;
	private final byte[] compressedBuffer;
	private final byte[] chunkHeader;
	private final LZ4SafeDecompressor decompressor;
	private final ChannelCache channelCache;
	private final ByteBufferPool writeBufferPool;
	private final boolean compressionEnabled;
	private int writeParallelism = 1;

	BucketedSequenceWriter(CloseSuppressPath root, long totalEntries, int bucketSize, int bufferRecords) {
		if (totalEntries < 0) {
			throw new IllegalArgumentException("totalEntries must be >= 0");
		}
		if (bucketSize <= 0) {
			throw new IllegalArgumentException("bucketSize must be > 0");
		}
		if (bufferRecords <= 0) {
			throw new IllegalArgumentException("bufferRecords must be > 0");
		}
		this.root = root;
		this.totalEntries = totalEntries;
		this.bucketSize = bucketSize;
		long bucketCountLong = Math.max(1L, (totalEntries + bucketSize - 1L) / bucketSize);
		if (bucketCountLong > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("bucketCount exceeds integer range: " + bucketCountLong);
		}
		this.bucketCount = (int) bucketCountLong;

		bucketBuffer = new int[bufferRecords];
		offsetBuffer = new int[bufferRecords];
		valueBuffer = new long[bufferRecords];

		bucketCounts = new int[bucketCount];
		bucketOffsets = new int[bucketCount + 1];
		bucketWriteCursor = new int[bucketCount];
		sortedOffsets = new int[bufferRecords];
		sortedValues = new long[bufferRecords];

		LZ4Compressor sizeCompressor = LZ4_FACTORY.fastCompressor();
		decompressor = LZ4_DECOMPRESSOR;
		ioBuffer = new byte[OBJECT_INDEX_IO_BUFFER_BYTES];
		compressedBuffer = new byte[sizeCompressor.maxCompressedLength(OBJECT_INDEX_IO_BUFFER_BYTES)];
		chunkHeader = new byte[OBJECT_INDEX_CHUNK_HEADER_BYTES];
		channelCache = new ChannelCache(MAX_CACHED_CHANNELS);
		writeBufferPool = new ByteBufferPool(WRITE_BUF_ROUNDING, WRITE_BUF_POOL_MAX_BYTES, WRITE_BUF_POOL_MAX_PER_SIZE,
				WRITE_BUF_POOL_MAX_BUFFER_BYTES);
		compressionEnabled = Lz4Config.ENABLED;
	}

	static BucketedSequenceWriter create(Path baseDir, String prefix, long totalEntries, int bucketSize,
			int bufferRecords) throws IOException {

		bucketSize = Math.max(bucketSize, OBJECT_INDEX_BUCKET_SIZE);
		bufferRecords = Math.max(bufferRecords, OBJECT_INDEX_BUFFER_RECORDS);

		Path rootPath;
		if (baseDir == null) {
			rootPath = Files.createTempDirectory(prefix);
		} else {
			rootPath = Files.createTempDirectory(baseDir, prefix + "-");
		}
		CloseSuppressPath root = CloseSuppressPath.of(rootPath);
		root.closeWithDeleteRecurse();
		return new BucketedSequenceWriter(root, totalEntries, bucketSize, bufferRecords);
	}

	static int resolveObjectIndexBucketSize(long totalEntries) {
		long bucketSize = Math.max(OBJECT_INDEX_BUCKET_SIZE, Math.min(totalEntries, (long) OBJECT_INDEX_BUCKET_SIZE));
		if (bucketSize > Integer.MAX_VALUE) {
			return Integer.MAX_VALUE;
		}
		return (int) bucketSize;
	}

	static int resolveObjectIndexBufferRecords(long totalEntries, int bucketSize) {
		long records = Math.max(OBJECT_INDEX_BUFFER_RECORDS,
				Math.min(totalEntries, Math.min((long) bucketSize, OBJECT_INDEX_BUFFER_RECORDS)));
		if (records > Integer.MAX_VALUE) {
			return Integer.MAX_VALUE;
		}
		return (int) records;
	}

	void setWriteParallelism(int parallelism) {
		if (parallelism < 1 || parallelism >= Integer.MAX_VALUE - 5) {
			throw new IllegalArgumentException("parallelism must be positive: " + parallelism);
		}
		this.writeParallelism = parallelism;
	}

	void add(long index, long value) {
		if (index < 0 || index >= totalEntries) {
			throw new IndexOutOfBoundsException("index out of bounds: " + index);
		}
		long bucketLong = index / bucketSize;
		int bucket = (int) bucketLong;
		int offset = (int) (index - (long) bucket * bucketSize);
		bucketBuffer[size] = bucket;
		offsetBuffer[size] = offset;
		valueBuffer[size] = value;
		size++;
		if (size == bucketBuffer.length) {
			try {
				flush();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	void addBatch(long[] indexes, long[] values, int length) {
		if (length <= 0) {
			return;
		}
		if (indexes == null || values == null) {
			throw new NullPointerException();
		}
		if (length > indexes.length || length > values.length) {
			throw new IndexOutOfBoundsException();
		}

		for (int i = 0; i < length; i++) {
			long index = indexes[i];
			if (index < 0 || index >= totalEntries) {
				throw new IndexOutOfBoundsException("index out of bounds: " + index);
			}
			long bucketLong = index / bucketSize;
			int bucket = (int) bucketLong;
			int offset = (int) (index - (long) bucket * bucketSize);
			bucketBuffer[size] = bucket;
			offsetBuffer[size] = offset;
			valueBuffer[size] = values[i];
			size++;
			if (size == bucketBuffer.length) {
				try {
					flush();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

	void flush() throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new RuntimeException("Thread interrupted");
		}
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
			sortedValues[outIndex] = valueBuffer[i];
		}

		if (shouldWriteInParallel()) {
			writeBucketsParallel();
		} else {
			WriteBuffers buffers = WRITE_BUFFERS.get();
			for (int bucket = 0; bucket < bucketCount; bucket++) {
				int start = bucketOffsets[bucket];
				int end = bucketOffsets[bucket + 1];
				if (start == end) {
					continue;
				}
				CloseSuppressPath file = root.resolve(bucketFileName(bucket));
				CachedChannel channel = null;
				try {
					channel = channelCache.acquire(bucket, file);
					writeRecords(channel, sortedOffsets, sortedValues, start, end, buffers);
					channelCache.release(bucket, channel);
				} catch (IOException e) {
					ChannelCache.closeQuietly(channel);
					throw e;
				}
			}
		}

		size = 0;
	}

	void materializeTo(DynamicSequence sequence) throws IOException {
		materializeTo(sequence, ProgressListener.ignore());
	}

	void materializeTo(DynamicSequence sequence, ProgressListener listener) throws IOException {
		ProgressListener progressListener = ProgressListener.ofNullable(listener);
		flush();
		if (totalEntries == 0) {
			progressListener.notifyProgress(100, "Materialized object index");
			return;
		}
		long processed = 0;
		int lastPercent = 0;
		long reportEvery = Math.max(1L, totalEntries / 100L);
		long nextReportAt = reportEvery;
		for (int bucket = 0; bucket < bucketCount; bucket++) {
			long bucketStart = (long) bucket * bucketSize;
			long remaining = totalEntries - bucketStart;
			if (remaining <= 0) {
				return;
			}
			int count = (int) Math.min(bucketSize, remaining);
			long[] values = LongArrayPool.borrow(count);
			Arrays.fill(values, 0, count, OBJECT_INDEX_MISSING);
			CloseSuppressPath file = root.resolve(bucketFileName(bucket));
			if (!Files.exists(file)) {
				throw new IllegalStateException("Missing bucket file: " + file);
			}
			try {
				try (InputStream in = new BufferedInputStream(Files.newInputStream(file),
						OBJECT_INDEX_IO_BUFFER_BYTES)) {
					readRecords(in, values);
				}

				for (int i = 0; i < count; i++) {
					long value = values[i];
					if (value == OBJECT_INDEX_MISSING) {
						throw new IllegalStateException("Missing mapping for index=" + (bucketStart + i));
					}
					sequence.set(bucketStart + i, value);
					processed++;
					if (processed >= nextReportAt) {
						int percent = (int) ((processed * 100d) / totalEntries);
						if (percent > lastPercent) {
							progressListener.notifyProgress(percent,
									"Materializing object index " + processed + "/" + totalEntries);
							lastPercent = percent;
						}
						nextReportAt += reportEvery;
					}
				}
			} finally {
				LongArrayPool.release(values);
			}
		}
		progressListener.notifyProgress(100, "Materialized object index");
	}

	long materializeCountsTo(DynamicSequence sequence, long entries, ProgressListener listener) throws IOException {
		ProgressListener progressListener = ProgressListener.ofNullable(listener);
		flush();
		long maxEntries = Math.max(0L, Math.min(entries, totalEntries));
		if (maxEntries == 0) {
			progressListener.notifyProgress(100, "Materialized object counts");
			return 0L;
		}
		long processed = 0;
		long reportEvery = Math.max(1L, maxEntries / 1000L);
		long nextReportAt = reportEvery;
		long maxCount = 0;
		for (int bucket = 0; bucket < bucketCount; bucket++) {
			long bucketStart = (long) bucket * bucketSize;
			long remaining = maxEntries - bucketStart;
			if (remaining <= 0) {
				break;
			}
			int count = (int) Math.min(bucketSize, remaining);
			long[] values = LongArrayPool.borrow(count);
			Arrays.fill(values, 0, count, 0L);
			CloseSuppressPath file = root.resolve(bucketFileName(bucket));
			try {
				if (Files.exists(file)) {
					try (InputStream in = new BufferedInputStream(Files.newInputStream(file),
							OBJECT_INDEX_IO_BUFFER_BYTES)) {
						readRecords(in, values, true);
					}
				}

				for (int i = 0; i < count; i++) {
					long value = values[i];
					if (value != 0) {
						sequence.set(bucketStart + i, value);
						if (value > maxCount) {
							maxCount = value;
						}
					}
					processed++;
					if (processed >= nextReportAt) {
						int percent = (int) ((processed * 100d) / maxEntries);
						progressListener.notifyProgress(percent,
								"Materializing object counts " + processed + "/" + maxEntries);
						nextReportAt += reportEvery;
					}
				}
			} finally {
				LongArrayPool.release(values);
			}
		}
		progressListener.notifyProgress(100, "Materialized object counts");
		return maxCount;
	}

	private void writeRecords(CachedChannel channel, int[] offsets, long[] values, int from, int to,
			WriteBuffers buffers) throws IOException {
		byte[] buffer = buffers.ioBuffer;
		int pos = 0;
		for (int i = from; i < to; i++) {
			if (buffer.length - pos < OBJECT_INDEX_RECORD_BYTES) {
				writeChunk(channel, buffer, pos, buffers);
				pos = 0;
			}
			putInt(buffer, pos, offsets[i]);
			pos += Integer.BYTES;
			putLong(buffer, pos, values[i]);
			pos += Long.BYTES;
		}
		if (pos > 0) {
			writeChunk(channel, buffer, pos, buffers);
		}
	}

	private void readRecords(InputStream in, long[] valuesByOffset) throws IOException {
		readRecords(in, valuesByOffset, false);
	}

	private void readRecords(InputStream in, long[] valuesByOffset, boolean accumulate) throws IOException {
		byte[] header = chunkHeader;
		byte[] buffer = ioBuffer;
		byte[] compressed = compressedBuffer;
		while (true) {
			int headerRead = readFully(in, header, 0, OBJECT_INDEX_CHUNK_HEADER_BYTES);
			if (headerRead == -1) {
				return;
			}
			if (headerRead < OBJECT_INDEX_CHUNK_HEADER_BYTES) {
				throw new EOFException(
						"Unexpected end of chunk header: " + headerRead + " < " + OBJECT_INDEX_CHUNK_HEADER_BYTES);
			}
			int uncompressedLength = getInt(header, 0);
			int compressedLength = getInt(header, Integer.BYTES);
			int payloadLength = compressedLength > 0 ? compressedLength : -compressedLength;
			if (uncompressedLength <= 0 || payloadLength <= 0) {
				throw new EOFException("Unexpected chunk lengths: uncompressed=" + uncompressedLength + " compressed="
						+ compressedLength);
			}
			if (uncompressedLength > buffer.length) {
				throw new IOException("Chunk too large: " + uncompressedLength + " > " + buffer.length);
			}
			if (compressedLength > 0 && compressedLength > compressed.length) {
				throw new IOException("Compressed chunk too large: " + compressedLength + " > " + compressed.length);
			}
			if (compressedLength < 0 && payloadLength != uncompressedLength) {
				throw new EOFException("Unexpected raw chunk length: " + payloadLength + " != " + uncompressedLength);
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
			if (uncompressedLength % OBJECT_INDEX_RECORD_BYTES != 0) {
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
			for (int pos = 0; pos < uncompressedLength; pos += OBJECT_INDEX_RECORD_BYTES) {
				int offset = getInt(buffer, pos);
				long value = getLong(buffer, pos + Integer.BYTES);
				if (offset < 0 || offset >= valuesByOffset.length) {
					throw new IOException("Offset out of range: " + offset + " >= " + valuesByOffset.length);
				}
				if (accumulate) {
					valuesByOffset[offset] += value;
				} else {
					if (valuesByOffset[offset] != OBJECT_INDEX_MISSING) {
						throw new IOException("Duplicate mapping for offset " + offset);
					}
					valuesByOffset[offset] = value;
				}
			}
		}
	}

	private void writeChunk(CachedChannel channel, byte[] buffer, int length, WriteBuffers buffers) throws IOException {
		int payloadCapacity = compressionEnabled ? buffers.compressor.maxCompressedLength(length) : length;
		int minCapacity = OBJECT_INDEX_CHUNK_HEADER_BYTES + payloadCapacity;
		ByteBuffer out = writeBufferPool.acquire(minCapacity);
		if (!out.hasArray()) {
			writeBufferPool.release(out);
			throw new IOException("Write buffer must be a heap ByteBuffer with accessible array");
		}

		byte[] outArray = out.array();
		int base = out.arrayOffset();
		int compressedLength;
		int payloadLength;
		if (compressionEnabled) {
			compressedLength = buffers.compressor.compress(buffer, 0, length, outArray,
					base + OBJECT_INDEX_CHUNK_HEADER_BYTES, outArray.length - (base + OBJECT_INDEX_CHUNK_HEADER_BYTES));
			payloadLength = compressedLength;
		} else {
			System.arraycopy(buffer, 0, outArray, base + OBJECT_INDEX_CHUNK_HEADER_BYTES, length);
			compressedLength = -length;
			payloadLength = length;
		}

		putInt(outArray, base, length);
		putInt(outArray, base + Integer.BYTES, compressedLength);

		int totalLen = OBJECT_INDEX_CHUNK_HEADER_BYTES + payloadLength;
		if (totalLen > out.capacity()) {
			log.error(
					"BucketedSequenceWriter chunk limit > capacity: totalLen={} capacity={} minCapacity={} "
							+ "payloadCapacity={} payloadLength={} compressionEnabled={} length={}",
					totalLen, out.capacity(), minCapacity, payloadCapacity, payloadLength, compressionEnabled, length);
		}
		out.clear();
		out.limit(totalLen);

		long writePos = channel.reserve(totalLen);
		try {
			writeFully(channel.channel, out, writePos);
		} finally {
			writeBufferPool.release(out);
		}
	}

	private static void writeFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
		long pos = position;
		while (buffer.hasRemaining()) {
			int written = channel.write(buffer, pos);
			if (written < 0) {
				throw new EOFException("FileChannel.write returned " + written);
			}
			pos += written;
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

	@Override
	public void close() throws IOException {
		IOException first = null;
		try {
			flush();
		} catch (IOException e) {
			first = e;
		}

		try {
			channelCache.close();
		} catch (IOException e) {
			if (first == null) {
				first = e;
			} else {
				first.addSuppressed(e);
			}
		} finally {
			writeBufferPool.clear();
		}

		try {
			root.close();
		} catch (IOException e) {
			if (first == null) {
				first = e;
			} else {
				first.addSuppressed(e);
			}
		}

		if (first != null) {
			throw first;
		}
	}

	private boolean shouldWriteInParallel() {
		return writeParallelism > 1 && bucketCount >= PARALLEL_WRITE_MIN_BUCKETS && size >= PARALLEL_WRITE_MIN_RECORDS;
	}

	private void writeBucketsParallel() throws IOException {
		ForkJoinPool pool = new ForkJoinPool(writeParallelism);
		List<ForkJoinTask<?>> tasks = new ArrayList<>();
		try {
			int taskCount = Math.min(bucketCount, Math.max(1, writeParallelism * 4));
			int chunkSize = (bucketCount + taskCount - 1) / taskCount;
			for (int task = 0; task < taskCount; task++) {
				int start = task * chunkSize;
				int end = Math.min(bucketCount, start + chunkSize);
				if (start >= end) {
					break;
				}
				tasks.add(pool.submit(() -> writeBucketRange(start, end)));
			}

			for (ForkJoinTask<?> task : tasks) {
				try {
					task.join();
				} catch (RuntimeException e) {
					Throwable cause = e.getCause();
					if (cause instanceof UncheckedIOException uio) {
						throw uio.getCause();
					}
					throw e;
				}
			}
		} finally {
			pool.shutdown();
		}
	}

	private void writeBucketRange(int startBucket, int endBucket) {
		WriteBuffers buffers = WRITE_BUFFERS.get();
		for (int bucket = startBucket; bucket < endBucket; bucket++) {
			int start = bucketOffsets[bucket];
			int end = bucketOffsets[bucket + 1];
			if (start == end) {
				continue;
			}
			notifyBucketWrite(bucket);
			CloseSuppressPath file = root.resolve(bucketFileName(bucket));
			CachedChannel channel = null;
			try {
				channel = channelCache.acquire(bucket, file);
				writeRecords(channel, sortedOffsets, sortedValues, start, end, buffers);
				channelCache.release(bucket, channel);
			} catch (IOException e) {
				ChannelCache.closeQuietly(channel);
				throw new UncheckedIOException(e);
			}
		}
	}

	private static void notifyBucketWrite(int bucket) {
		BucketWriteObserver observer = bucketWriteObserver;
		if (observer != null) {
			observer.onBucketWrite(bucket, Thread.currentThread().getName());
		}
	}

	private static final class CachedChannel {
		final FileChannel channel;
		long position;

		CachedChannel(FileChannel channel, long position) {
			this.channel = channel;
			this.position = position;
		}

		long reserve(int bytes) {
			long p = position;
			position += bytes;
			return p;
		}
	}

	private static final class ChannelCache implements Closeable {
		private final int maxCached;
		private final LinkedHashMap<Integer, CachedChannel> lru;
		private final Object lock = new Object();

		ChannelCache(int maxCached) {
			this.maxCached = Math.max(0, maxCached);
			this.lru = new LinkedHashMap<>(16, 0.75f, true);
		}

		CachedChannel acquire(int bucket, CloseSuppressPath file) throws IOException {
			CachedChannel cached;
			synchronized (lock) {
				cached = lru.remove(bucket);
			}
			if (cached != null && cached.channel != null && cached.channel.isOpen()) {
				return cached;
			}
			closeQuietly(cached);

			FileChannel channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
			long pos = channel.size();
			return new CachedChannel(channel, pos);
		}

		void release(int bucket, CachedChannel ch) {
			if (ch == null || ch.channel == null || !ch.channel.isOpen() || maxCached == 0) {
				closeQuietly(ch);
				return;
			}
			CachedChannel toClose = null;
			synchronized (lock) {
				lru.put(bucket, ch);
				if (lru.size() > maxCached) {
					Map.Entry<Integer, CachedChannel> eldest = lru.entrySet().iterator().next();
					lru.remove(eldest.getKey());
					toClose = eldest.getValue();
				}
			}
			closeQuietly(toClose);
		}

		@Override
		public void close() throws IOException {
			List<CachedChannel> toClose;
			synchronized (lock) {
				toClose = new ArrayList<>(lru.values());
				lru.clear();
			}

			IOException first = null;
			for (CachedChannel ch : toClose) {
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
			if (first != null) {
				throw first;
			}
		}

		static void closeQuietly(CachedChannel ch) {
			if (ch == null || ch.channel == null) {
				return;
			}
			try {
				ch.channel.close();
			} catch (IOException ignored) {
			}
		}
	}

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
				return;
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

	private static final class WriteBuffers {
		private final byte[] ioBuffer;
		private final LZ4Compressor compressor;

		private WriteBuffers() {
			compressor = LZ4_FACTORY.fastCompressor();
			ioBuffer = new byte[OBJECT_INDEX_IO_BUFFER_BYTES];
		}
	}

	private static String bucketFileName(int bucket) {
		if (bucket < 0 || bucket >= 1_000_000) {
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
		c[1] = (char) ('0' + x);

		return new String(c);
	}

	private static final String ZEROS_6 = "000000";

	private static String bucketFileNameSlowButGeneral(int bucket) {
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
}
