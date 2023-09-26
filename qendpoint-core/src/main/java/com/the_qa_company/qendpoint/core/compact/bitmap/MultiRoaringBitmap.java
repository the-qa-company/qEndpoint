package com.the_qa_company.qendpoint.core.compact.bitmap;

import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.io.CloseMappedByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import org.roaringbitmap.RoaringBitmap;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static java.lang.String.format;

/**
 * {@link ModifiableBitmap} using multiple roaring bitmap to create a 64bits version, can't be loaded.
 *
 * @author Antoine Willerval
 */
public class MultiRoaringBitmap implements SimpleModifiableBitmap, Closeable {
	// cookie + maps_nb + chunk_size
	private static final int HEADER_SIZE = 8 + 4 + 4;
	public static final long COOKIE = 0x6347008534687531L;

	/**
	 * load mapped multi roaring bitmap stream
	 *
	 * @param input stream
	 * @return bitmap
	 * @throws IOException io exception when loading
	 */
	public static MultiRoaringBitmap load(InputStream input) throws IOException {
		return new MultiRoaringBitmap(input);
	}

	/**
	 * load mapped multi roaring bitmap file
	 *
	 * @param path file
	 * @return bitmap
	 * @throws IOException io exception when loading
	 */
	public static MultiRoaringBitmap mapped(Path path) throws IOException {
		return mapped(path, 0);
	}

	/**
	 * load mapped multi roaring bitmap file
	 *
	 * @param path file
	 * @return bitmap
	 * @throws IOException io exception when loading
	 */
	public static MultiRoaringBitmap mapped(Path path, long start) throws IOException {
		try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
			return mapped(path, start, channel);
		}
	}

	/**
	 * load mapped multi roaring bitmap file.
	 *
	 * @param fileName file name for leak tracking
	 * @param start    channel start
	 * @param channel  channel to read
	 * @return bitmap
	 * @throws IOException io exception when loading
	 */
	public static MultiRoaringBitmap mapped(Path fileName, long start, FileChannel channel) throws IOException {
		return new MultiRoaringBitmap(fileName, channel, start);
	}

	/**
	 * create a multi roaring bitmap with a size with the default chunk size.
	 *
	 * @param size size
	 * @return bitmap
	 */
	public static MultiRoaringBitmap memory(long size) {
		return memory(size, defaultChunkSize);
	}

	/**
	 * create a multi roaring bitmap with a size.
	 *
	 * @param size      size
	 * @param chunkSize chunk size
	 * @return bitmap
	 */
	public static MultiRoaringBitmap memory(long size, int chunkSize) {
		try {
			return new MultiRoaringBitmap(size, chunkSize, null);
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}


	/**
	 * create a multi roaring bitmap with a size with the default chunk size for stream writing.
	 *
	 * @param size         size
	 * @param streamOutput stream output
	 * @return bitmap
	 */
	public static MultiRoaringBitmap memoryStream(long size, Path streamOutput) throws IOException {
		return memoryStream(size, defaultChunkSize, streamOutput);
	}


	/**
	 * create a multi roaring bitmap with a size for stream writing.
	 *
	 * @param size         size
	 * @param chunkSize    chunk size
	 * @param streamOutput stream output
	 * @return bitmap
	 */
	public static MultiRoaringBitmap memoryStream(long size, int chunkSize, Path streamOutput) throws IOException {
		return new MultiRoaringBitmap(size, chunkSize, streamOutput);
	}

	static int defaultChunkSize = 1 << 29;
	final List<Bitmap> maps = new ArrayList<>();
	final int chunkSize;
	private final boolean writable;
	private final FileChannel output;
	private final Path outputPath;
	private long outputMax;

	private MultiRoaringBitmap(InputStream input) throws IOException {
		ByteBuffer buffer = ByteBuffer.wrap(IOUtil.readBuffer(input, HEADER_SIZE, ProgressListener.ignore()))
				.order(ByteOrder.LITTLE_ENDIAN);

		long cookie = buffer.getLong(0);
		if (cookie != COOKIE) {
			throw new IOException(format("found bad cookie %x != %x", cookie, COOKIE));
		}

		int chunks = buffer.getInt(8);
		chunkSize = buffer.getInt(12);
		writable = true;
		output = null;
		outputPath = null;

		for (int i = 0; i < chunks; i++) {
			input.skipNBytes(8); // skip size used for mapping

			RoaringBitmap32 bitmap32 = new RoaringBitmap32();
			bitmap32.getHandle().deserialize(new DataInputStream(input));
			maps.add(bitmap32);
		}

	}

	private MultiRoaringBitmap(long size, int chunkSize, Path output) throws IOException {
		writable = true;
		if (size < 0) {
			throw new IllegalArgumentException("Negative size: " + size);
		}
		this.chunkSize = chunkSize;

		int chunks = (int) ((size - 1) / chunkSize + 1);

		try {
			if (output != null) {
				this.outputPath = output;
				this.output = FileChannel.open(output, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);

				try (CloseMappedByteBuffer map = IOUtil.mapChannel(output, this.output, FileChannel.MapMode.READ_WRITE, 0, HEADER_SIZE)) {
					map.order(ByteOrder.LITTLE_ENDIAN);
					map.putLong(0, COOKIE);
					map.putInt(8, chunks);
					map.putInt(12, chunkSize);
				}

				outputMax = HEADER_SIZE;
			} else {
				this.output = null;
				this.outputPath = null;
			}

			for (int i = 0; i < chunks; i++) {
				maps.add(new RoaringBitmap32()); // to on use?
			}
		} catch (Throwable t) {
			try {
				close();
			} catch (Exception e) {
				t.addSuppressed(e);
			} catch (Throwable t2) {
				t2.addSuppressed(t);
				throw t2;
			}
			throw t;
		}
	}

	private MultiRoaringBitmap(Path fileName, FileChannel channel, long start) throws IOException {
		writable = false;
		output = null;
		this.outputPath = null;
		try (
				CloseMappedByteBuffer header = IOUtil.mapChannel(fileName, channel, FileChannel.MapMode.READ_ONLY, start, HEADER_SIZE)
		) {
			header.order(ByteOrder.LITTLE_ENDIAN);

			long cookie = header.getLong(0);
			if (cookie != COOKIE) {
				throw new IOException(format("Bad cookie for multi roaring bitmap %x != %x", cookie, COOKIE));
			}
			int bitmapCount = header.getInt(8);
			chunkSize = header.getInt(12);

			long shift = HEADER_SIZE + start;
			for (int i = 0; i < bitmapCount; i++) {
				long sizeBytes = IOUtil.readLong(shift, channel, ByteOrder.LITTLE_ENDIAN);
				maps.add(new MappedRoaringBitmap(IOUtil.mapChannel(fileName, channel, FileChannel.MapMode.READ_ONLY, shift += 8, sizeBytes)));
				shift += sizeBytes;
			}
		} catch (Throwable t) {
			try {
				close();
			} catch (Exception e) {
				t.addSuppressed(e);
			} catch (Throwable t2) {
				t2.addSuppressed(t);
				throw t2;
			}
			throw t;
		}
	}

	private void closeStreamBitmap(int index) throws IOException {
		Bitmap map = maps.get(index);
		if (map == null) {
			return;
		}

		if (!(map instanceof RoaringBitmap32 rbm)) {
			throw new AssertionError();
		}

		RoaringBitmap handle = rbm.getHandle();

		long loc = outputMax;
		int sizeInBytes = handle.serializedSizeInBytes();
		outputMax += sizeInBytes + 8;

		try (CloseMappedByteBuffer buffer = IOUtil.mapChannel(outputPath, output, FileChannel.MapMode.READ_WRITE, loc, sizeInBytes + 8)) {
			ByteBuffer internalBuffer = buffer.getInternalBuffer().order(ByteOrder.LITTLE_ENDIAN);
			internalBuffer.putLong(0, sizeInBytes);
			handle.serialize(internalBuffer.slice(8, sizeInBytes));
		}

		try {
			Closer.closeSingle(map);
		} finally {
			maps.set(index, null);
			System.gc();
		}
	}

	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		if (this.output != null) {
			throw new IllegalArgumentException("Can't save a streamed bitmap");
		}
		if (!writable) {
			throw new IllegalArgumentException("Can't save mapped bitmap");
		}

		// compute headers
		byte[] bytes = new byte[HEADER_SIZE];
		ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

		buffer.putLong(0, COOKIE);
		buffer.putInt(8, maps.size());
		buffer.putInt(12, chunkSize);

		output.write(bytes);

		for (Bitmap map : maps) {
			RoaringBitmap handle = ((RoaringBitmap32) map).getHandle();

			int sizeInBytes = handle.serializedSizeInBytes();
			byte[] array = new byte[8];
			ByteBuffer.wrap(array)
					.order(ByteOrder.LITTLE_ENDIAN)
					.putLong(0, sizeInBytes);
			output.write(array);

			handle.serialize(new DataOutputStream(output));
		}
	}

	@Override
	public boolean access(long position) {
		int location = (int) (position / chunkSize);
		if (location >= maps.size() || position < 0) {
			return false;
		}
		int localLocation = (int) (position % chunkSize);
		return maps.get(location).access(localLocation);
	}

	@Override
	public long getNumBits() {
		return maps.stream().mapToLong(Bitmap::getNumBits).sum();
	}

	@Override
	public long getSizeBytes() {
		return HEADER_SIZE
		       + maps.stream().mapToLong(Bitmap::getSizeBytes).sum();
	}

	@Override
	public String getType() {
		return HDTVocabulary.BITMAP_TYPE_ROARING_MULTI;
	}

	@Override
	public long countOnes() {
		return maps.stream().mapToLong(Bitmap::countOnes).sum();
	}

	@Override
	public long select1(long n) {
		long count = n;
		long delta = 0;
		int idx = 0;

		while (idx < maps.size()) {
			long countOnes = maps.get(idx).countOnes();
			if (count <= countOnes) {
				break;
			}
			count -= countOnes;
			delta += idx != maps.size() - 1 ? chunkSize : maps.get(idx).getNumBits();
			idx++;
		}

		if (idx == maps.size()) {
			if (maps.isEmpty()) {
				return 0;
			}
			return delta;
		}

		return delta + maps.get(idx).select1(count);
	}

	@Override
	public long rank1(long position) {
		int location = (int) (position / chunkSize);

		if (location >= maps.size() || position < 0) {
			return 0;
		}

		int localLocation = (int) (position % chunkSize);

		long delta = 0;
		for (int i = 0; i < location; i++) {
			delta += maps.get(i).getNumBits();
		}

		return delta + maps.get(location).rank1(localLocation);
	}

	@Override
	public long selectPrev1(long start) {
		return select1(rank1(start));
	}

	@Override
	public long selectNext1(long start) {
		long pos = rank1(start - 1);
		if (pos < getNumBits())
			return select1(pos + 1);
		return -1;
	}


	@Override
	public void close() throws IOException {
		try {
			if (output != null) {
				// write remaining
				Closer.closeAll(IntStream.range(0, maps.size()).mapToObj(
						index -> (Closeable) (() -> closeStreamBitmap(index))
				));
			}
		} finally {
			Closer.closeAll(maps, output);
		}
	}

	@Override
	public void set(long position, boolean value) {
		if (!writable) {
			throw new IllegalArgumentException("not writable");
		}

		int location = (int) (position / chunkSize);
		if (location >= maps.size() || position < 0) {
			throw new IllegalArgumentException(format("bit outside of range %d < 0 ||  map(%d)=%d >= %d",
					position, position, location, maps.size()));
		}
		int localLocation = (int) (position % chunkSize);

		if (output != null) { // streaming
			if (maps.get(location) == null) {
				throw new IllegalArgumentException("Passing unsorted values in streaming mode");
			}
			// clear previous
			try {
				Closer.closeAll(IntStream.range(0, location).mapToObj(
						index -> (Closeable) (() -> closeStreamBitmap(index))
				));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		// set the bit
		((ModifiableBitmap) maps.get(location)).set(localLocation, value);
	}

	@Override
	public void append(boolean value) {
		throw new NotImplementedException();
	}
}
