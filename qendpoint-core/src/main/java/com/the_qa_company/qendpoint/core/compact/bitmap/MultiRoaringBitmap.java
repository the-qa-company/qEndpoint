package com.the_qa_company.qendpoint.core.compact.bitmap;

import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.storage.TempBuffIn;
import com.the_qa_company.qendpoint.core.storage.TempBuffOut;
import com.the_qa_company.qendpoint.core.util.io.CloseMappedByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import org.roaringbitmap.RoaringBitmap;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.lang.String.format;

/**
 * {@link ModifiableBitmap} using multiple roaring bitmap to create a 64bits
 * version, can't be loaded.
 *
 * @author Antoine Willerval
 */
public class MultiRoaringBitmap implements Closeable, ModifiableMultiLayerBitmap {
	// cookie + maps_nb + chunk_size + numbits + num_layers
	private static final int HEADER_SIZE = 8 + 4 + 4 + 8 + 8;
	public static final long COOKIE = 0x6347008534687532L;

	// End of the blocks
	public static final byte BLOCK_END = 0x40;
	// Bitmap block
	public static final byte BLOCK_BITMAP = 0x41;

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
	 * load mapped multi roaring bitmap stream
	 *
	 * @param input stream
	 * @return bitmap
	 * @throws IOException io exception when loading
	 */
	public static MultiRoaringBitmap load(Path input) throws IOException {
		try (InputStream stream = new TempBuffIn(Files.newInputStream(input))) {
			return load(stream);
		}
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
	public static MultiRoaringBitmap memory(long size, long layers) {
		return memory(size, layers, defaultChunkSize);
	}

	/**
	 * create a multi roaring bitmap with a size.
	 *
	 * @param size      size
	 * @param chunkSize chunk size
	 * @return bitmap
	 */
	public static MultiRoaringBitmap memory(long size, long layers, int chunkSize) {
		try {
			return new MultiRoaringBitmap(size, layers, chunkSize, null);
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}

	/**
	 * create a multi roaring bitmap with a size with the default chunk size for
	 * stream writing.
	 *
	 * @param size         size
	 * @param streamOutput stream output
	 * @return bitmap
	 */
	public static MultiRoaringBitmap memoryStream(long size, long layers, Path streamOutput) throws IOException {
		return memoryStream(size, layers, defaultChunkSize, streamOutput);
	}

	/**
	 * create a multi roaring bitmap with a size for stream writing.
	 *
	 * @param size         size
	 * @param chunkSize    chunk size
	 * @param streamOutput stream output
	 * @return bitmap
	 */
	public static MultiRoaringBitmap memoryStream(long size, long layers, int chunkSize, Path streamOutput)
			throws IOException {
		return new MultiRoaringBitmap(size, layers, chunkSize, streamOutput);
	}

	static int defaultChunkSize = 1 << 29;
	final List<List<Bitmap>> maps = new ArrayList<>();
	final int chunks;
	final int chunkSize;
	final long layers;
	private final long numbits;
	private final boolean writable;
	private final FileChannel output;
	private long outputMax;
	private boolean closed;

	private MultiRoaringBitmap(InputStream input) throws IOException {
		ByteBuffer buffer = ByteBuffer.wrap(IOUtil.readBuffer(input, HEADER_SIZE, ProgressListener.ignore()))
				.order(ByteOrder.LITTLE_ENDIAN);

		long cookie = buffer.getLong(0);
		if (cookie != COOKIE) {
			throw new IOException(format("found bad cookie %x != %x", cookie, COOKIE));
		}

		chunks = buffer.getInt(8);
		chunkSize = buffer.getInt(12);
		numbits = buffer.getLong(16);
		layers = buffer.getLong(24);
		writable = true;
		output = null;

		int type;
		while ((type = input.read()) != BLOCK_END) {
			switch (type) {
			case BLOCK_BITMAP -> {
				input.skipNBytes(Long.BYTES); // skip size used for mapping
				long layer = IOUtil.readLong(input);

				if (layer < 0) {
					throw new IOException("Found negative layer!");
				}

				// generate the layer
				while (layer >= maps.size()) {
					maps.add(new ArrayList<>());
				}

				List<Bitmap> map = maps.get((int) layer);
				RoaringBitmap32 bitmap32 = new RoaringBitmap32();
				bitmap32.getHandle().deserialize(new DataInputStream(input));
				map.add(bitmap32);
			}
			case -1 -> throw new EOFException();
			default -> throw new IOException(format("Found bad type format %x", type));
			}
		}
	}

	private MultiRoaringBitmap(long size, long layers, int chunkSize, Path output) throws IOException {
		writable = true;
		if (size < 0) {
			throw new IllegalArgumentException("Negative size: " + size);
		}
		this.chunkSize = chunkSize;
		this.layers = layers;
		this.numbits = size;

		chunks = (int) ((size - 1) / chunkSize + 1);

		try {
			if (output != null) {
				this.output = FileChannel.open(output, StandardOpenOption.READ, StandardOpenOption.WRITE,
						StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);

				try (CloseMappedByteBuffer map = IOUtil.mapChannel(output, this.output, FileChannel.MapMode.READ_WRITE,
						0, HEADER_SIZE)) {
					map.order(ByteOrder.LITTLE_ENDIAN);
					map.putLong(0, COOKIE);
					map.putInt(8, chunks);
					map.putInt(12, chunkSize);
					map.putLong(16, size);
					map.putLong(24, layers);
				}

				outputMax = HEADER_SIZE;
			} else {
				this.output = null;
			}

			for (int j = 0; j < layers; j++) {
				List<Bitmap> map = new ArrayList<>();
				maps.add(map);
				for (int i = 0; i < chunks; i++) {
					map.add(new RoaringBitmap32()); // to on use?
				}
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
		try {
			try (CloseMappedByteBuffer header = IOUtil.mapChannel(fileName, channel, FileChannel.MapMode.READ_ONLY,
					start, HEADER_SIZE)) {
				header.order(ByteOrder.LITTLE_ENDIAN);

				long cookie = header.getLong(0);
				if (cookie != COOKIE) {
					throw new IOException(format("Bad cookie for multi roaring bitmap %x != %x", cookie, COOKIE));
				}
				chunks = header.getInt(8);
				chunkSize = header.getInt(12);
				numbits = header.getLong(16);
				layers = header.getLong(24);

				for (int i = 0; i < layers; i++) {
					maps.add(new ArrayList<>());
				}
			}

			long shift = HEADER_SIZE + start;

			int type;
			while (true) {
				InputStream stream = Channels.newInputStream(channel.position(shift));
				if ((type = stream.read()) == BLOCK_END) {
					break;
				}
				shift++;

				switch (type) {
				case BLOCK_BITMAP -> {
					long sizeBytes = IOUtil.readLong(stream);
					long layer = IOUtil.readLong(stream);
					shift += 8 + 8;
					MappedRoaringBitmap bm = new MappedRoaringBitmap(
							IOUtil.mapChannel(fileName, channel, FileChannel.MapMode.READ_ONLY, shift, sizeBytes));
					maps.get((int) layer).add(bm);
					shift += sizeBytes;
				}
				case -1 -> throw new EOFException();
				default -> throw new IOException(format("unknown type %x", type));
				}
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

	private void closeStreamBitmap(int layer, int index) throws IOException {
		Bitmap map = maps.get(layer).get(index);
		if (map == null) {
			return;
		}

		if (!(map instanceof RoaringBitmap32 rbm)) {
			throw new AssertionError();
		}

		RoaringBitmap handle = rbm.getHandle();

		long loc = outputMax;
		int sizeInBytes = handle.serializedSizeInBytes();
		outputMax += sizeInBytes + 8 + 8 + 1;

		OutputStream os = new TempBuffOut(Channels.newOutputStream(output.position(loc)));
		os.write(BLOCK_BITMAP);
		IOUtil.writeLong(os, sizeInBytes);
		IOUtil.writeLong(os, layer);
		handle.serialize(new DataOutputStream(os));
		os.flush();

		try {
			Closer.closeSingle(map);
		} finally {
			maps.get(layer).set(index, null);
		}
	}

	public void save(Path output) throws IOException {
		try (OutputStream stream = new TempBuffOut(Files.newOutputStream(output))) {
			save(stream);
		}
	}

	public void save(OutputStream output) throws IOException {
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
		buffer.putLong(16, numbits);
		buffer.putLong(24, maps.size());

		output.write(bytes);

		for (int i = 0; i < maps.size(); i++) {
			// put the maps sequentially, maybe to test by putting the chunks
			// closer?
			for (Bitmap map : maps.get(i)) {
				RoaringBitmap handle = ((RoaringBitmap32) map).getHandle();

				output.write(BLOCK_BITMAP);
				int sizeInBytes = handle.serializedSizeInBytes();
				IOUtil.writeLong(output, sizeInBytes);
				IOUtil.writeLong(output, i); // layer

				handle.serialize(new DataOutputStream(output));
			}
		}
		output.write(BLOCK_END);
	}

	@Override
	public boolean access(long graph, long position) {
		int location = (int) (position / chunkSize);
		List<Bitmap> maps = this.maps.get((int) graph);
		if (location >= maps.size() || position < 0) {
			return false;
		}
		int localLocation = (int) (position % chunkSize);
		return maps.get(location).access(localLocation);
	}

	@Override
	public long getNumBits() {
		return numbits;
	}

	@Override
	public long getSizeBytes() {
		return HEADER_SIZE + maps.stream().flatMap(Collection::stream).mapToLong(Bitmap::getSizeBytes).sum();
	}

	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		save(output);
	}

	@Override
	public void load(InputStream input, ProgressListener listener) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public String getType() {
		return HDTVocabulary.BITMAP_TYPE_ROARING_MULTI;
	}

	@Override
	public long getLayersCount() {
		return maps.size();
	}

	@Override
	public long countOnes(long graph) {
		return maps.get((int) graph).stream().mapToLong(Bitmap::countOnes).sum();
	}

	@Override
	public long countZeros(long layer) {
		throw new NotImplementedException();
	}

	@Override
	public long select1(long graph, long n) {
		long count = n;
		long delta = 0;
		int idx = 0;

		List<Bitmap> map = maps.get((int) graph);
		while (true) {
			if (!(idx < map.size()))
				break;
			long countOnes = map.get(idx).countOnes();
			if (count <= countOnes) {
				break;
			}
			count -= countOnes;
			delta += idx != map.size() - 1 ? chunkSize : map.get(idx).getNumBits();
			idx++;
		}

		if (idx == map.size()) {
			if (map.isEmpty()) {
				return 0;
			}
			return delta;
		}

		return delta + map.get(idx).select1(count);
	}

	@Override
	public long rank1(long graph, long position) {
		List<Bitmap> map = maps.get((int) graph);
		int location = (int) (position / chunkSize);

		if (location >= map.size() || position < 0) {
			return 0;
		}

		int localLocation = (int) (position % chunkSize);

		long delta = 0;
		for (int i = 0; i < location; i++) {
			delta += map.get(i).getNumBits();
		}

		return delta + map.get(location).rank1(localLocation);
	}

	@Override
	public long rank0(long layer, long position) {
		return position + 1L - rank1(layer, position);
	}

	@Override
	public long selectPrev1(long graph, long start) {
		return select1(graph, rank1(graph, start));
	}

	@Override
	public long selectNext1(long graph, long start) {
		long pos = rank1(graph, start - 1);
		if (pos < getNumBits())
			return select1(graph, pos + 1);
		return -1;
	}

	@Override
	public long select0(long layer, long n) {
		throw new NotImplementedException();
	}

	@Override
	public void close() throws IOException {
		if (closed) {
			return;
		}
		closed = true;
		try {
			if (output != null) {
				// write remaining
				Closer.closeAll(IntStream.range(0, maps.size())
						.mapToObj(layer -> IntStream.range(0, maps.get(layer) == null ? 0 : maps.get(layer).size())
								.mapToObj(index -> (Closeable) (() -> closeStreamBitmap(layer, index))))
						.flatMap(Function.identity()));

				OutputStream os = Channels.newOutputStream(output.position(outputMax++));
				os.write(BLOCK_END);
				os.flush();
			}
		} finally {
			Closer.closeAll(maps, output);
		}
	}

	@Override
	public void set(long layer, long position, boolean value) {
		if (!writable) {
			throw new IllegalArgumentException("not writable");
		}

		if (layer >= maps.size()) {
			for (int i = 0; i <= layer; i++) {
				List<Bitmap> map = new ArrayList<>();
				maps.add(map);
				for (int j = 0; j < chunks; j++) {
					map.add(new RoaringBitmap32()); // to on use?
				}
			}
		}
		List<Bitmap> maps = this.maps.get((int) layer);

		int location = (int) (position / chunkSize);
		if (location >= maps.size() || position < 0) {
			throw new IllegalArgumentException(format("bit outside of range %d < 0 ||  map(%d)=%d >= %d", position,
					position, location, maps.size()));
		}
		int localLocation = (int) (position % chunkSize);

		if (output != null) { // streaming
			if (maps.get(location) == null) {
				throw new IllegalArgumentException("Passing unsorted values in streaming mode");
			}
			// clear previous
			try {
				Closer.closeAll(IntStream.range(0, location)
						.mapToObj(index -> (Closeable) (() -> closeStreamBitmap((int) layer, index))));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		// set the bit
		((ModifiableBitmap) maps.get(location)).set(localLocation, value);
	}
}
