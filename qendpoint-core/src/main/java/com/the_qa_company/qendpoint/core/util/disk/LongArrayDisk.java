/**
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; version 3.0 of the License.
 * <p>
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * <p>
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 * <p>
 * Contacting the authors: Dennis Diefenbach:
 * dennis.diefenbach@univ-st-etienne.fr Jose Gimenez Garcia:
 * jose.gimenez.garcia@univ-st-etienne.fr
 */

package com.the_qa_company.qendpoint.core.util.disk;

import com.the_qa_company.qendpoint.core.util.io.CloseMappedByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

//Implementing an array of longs that is backed up on disk. Following this: http://vanillajava.blogspot.fr/2011/12/using-memory-mapped-file-for-huge.html

public class LongArrayDisk extends AbstractLongArray implements Closeable {
	private static final long MAPPING_SIZE = 1 << 30;
	private final boolean closeChannel;
	private final FileChannel channel;
	private CloseMappedByteBuffer[] mappings;
	private long size;
	private final long startByte;
	private final Path location;

	public LongArrayDisk(String location, long size) {
		this(location, size, true);
	}

	public LongArrayDisk(String location, long size, boolean overwrite) {
		this(Path.of(location), size, overwrite);
	}

	public LongArrayDisk(Path location, long size) {
		this(location, size, true);
	}

	public LongArrayDisk(Path location, long size, boolean overwrite) {
		this(location, size, overwrite, 0);
	}

	public LongArrayDisk(Path location, long size, boolean overwrite, long start) {
		this(location, null, true, size, overwrite, start);
	}

	public LongArrayDisk(Path location, FileChannel channel, boolean closeChannel, long size, boolean overwrite,
			long start) {
		this.location = location;
		this.startByte = start;
		try {
			this.size = size;
			if (channel == null) {
				Files.createDirectories(location.getParent());
				this.channel = FileChannel.open(location, StandardOpenOption.READ, StandardOpenOption.WRITE,
						StandardOpenOption.CREATE);
				this.closeChannel = true;
			} else {
				this.channel = channel;
				this.closeChannel = closeChannel;
			}
			long sizeBytes = getSizeBytes();
			int blocks = (int) Math.ceil((double) sizeBytes / MAPPING_SIZE);
			mappings = new CloseMappedByteBuffer[blocks];
			for (int block = 0; block < blocks; block++) {
				long sizeMapping;
				if (block + 1 == blocks && sizeBytes % MAPPING_SIZE != 0) {
					sizeMapping = Math.min(MAPPING_SIZE, sizeBytes % MAPPING_SIZE);
				} else {
					sizeMapping = MAPPING_SIZE;
				}
				assert mappings[block] == null;
				mappings[block] = IOUtil.mapChannel(location.toAbsolutePath().toString(), this.channel,
						FileChannel.MapMode.READ_WRITE, start + block * MAPPING_SIZE, sizeMapping);
			}
			if (overwrite) {
				clear();
			}
		} catch (IOException e) {
			try {
				try {
					if (mappings != null) {
						IOUtil.closeAll(mappings);
					}
				} finally {
					if (closeChannel && channel != null) {
						channel.close();
					}
				}
			} catch (IOException e1) {
				e.addSuppressed(e1);
			}
			throw new RuntimeException("can't create LongArrayDisk!", e);
		}
	}

	/**
	 * Allows the {@link RandomAccessFile} and the array of
	 * {@link MappedByteBuffer} held by the instance to be garbage-collected.
	 */
	@Override
	public void close() throws IOException {
		try {
			IOUtil.closeAll(mappings);
		} finally {
			mappings = null;
			if (closeChannel) {
				channel.close();
			}
		}
	}

	@Override
	public long get(long x) {
		long p = x * 8;
		int block = (int) (p / MAPPING_SIZE);
		int offset = (int) (p % MAPPING_SIZE);
		return mappings[block].getLong(offset);
	}

	@Override
	protected void innerSet(long index, long value) {
		if (index >= size || index < 0) {
			throw new IndexOutOfBoundsException();
		}
		long p = index * 8;
		int block = (int) (p / MAPPING_SIZE);
		int offset = (int) (p % MAPPING_SIZE);
		mappings[block].putLong(offset, value);
	}

	@Override
	public long length() {
		return size;
	}

	/**
	 * @return the start byte of this array in the file
	 */
	public long getStartByte() {
		return startByte;
	}

	/**
	 * @return the byte after end of this array in the file, equals to
	 *         {@link #getStartByte()} + {@link #getSizeBytes()}
	 */
	public long getEndByte() {
		return getStartByte() + getSizeBytes();
	}

	@Override
	public int sizeOf() {
		return Long.SIZE;
	}

	public void set0(long startIndex, long endIndex) {
		long start = startIndex * 8;
		long end = endIndex * 8;

		if (start >= end) {
			return;
		}
		int startBlock = (int) (start / MAPPING_SIZE);
		int endBlock = (int) ((end - 1) / MAPPING_SIZE);

		// 0 buffer
		byte[] zeros = new byte[4096];

		int c = 0;
		int lastBlock = startBlock;
		for (int i = startBlock; i <= endBlock; i++) {
			CloseMappedByteBuffer mapping = mappings[i];
			int capacity = mapping.capacity();

			int startBuffer;
			int endBuffer;

			if (i == startBlock) {
				startBuffer = (int) (start % MAPPING_SIZE);
			} else {
				startBuffer = 0;
			}
			if (i == endBlock) {
				if (end % MAPPING_SIZE == 0) {
					endBuffer = (int) MAPPING_SIZE;
				} else {
					endBuffer = (int) (end % MAPPING_SIZE);
				}
			} else {
				endBuffer = capacity;
			}

			assert endBuffer > startBuffer : "start after end " + startBuffer + " -> " + endBuffer;

			int toWrite = endBuffer - startBuffer;

			int shift = 0;
			while (toWrite > 0) {
				mapping.position(startBuffer + shift);
				int w = Math.min(toWrite, zeros.length);

				mapping.put(zeros, 0, w);

				shift += w;
				toWrite -= w;
				c += w;
				if (c > 10000000) {
					c = 0;
					for (int j = lastBlock; j < i; j++) {
						mappings[j].force();
					}
					lastBlock = i;
				}
			}
		}
		for (int j = lastBlock; j <= endBlock; j++) {
			mappings[j].force();
		}
	}

	@Override
	public void resize(long newSize) throws IOException {
		if (this.size == newSize) {
			return;
		}
		long oldSize = this.size;
		this.size = newSize;
		long sizeBytes = getSizeBytes();

		int blocks = (int) Math.ceil((double) sizeBytes / MAPPING_SIZE);
		CloseMappedByteBuffer[] mappings = new CloseMappedByteBuffer[blocks];
		try {
			for (CloseMappedByteBuffer mapping : this.mappings) {
				if (mapping != null) {
					mapping.force();
				}
			}
			IOUtil.closeAll(this.mappings);
			this.mappings = null;

			// resize the file to the new size
			channel.truncate(sizeBytes);

			for (int block = 0; block < blocks; block++) {
				long sizeMapping;
				if (block + 1 == blocks && sizeBytes % MAPPING_SIZE != 0) {
					sizeMapping = Math.min(MAPPING_SIZE, sizeBytes % MAPPING_SIZE);
				} else {
					sizeMapping = MAPPING_SIZE;
				}
				mappings[block] = IOUtil.mapChannel(location.toAbsolutePath().toString(), channel,
						FileChannel.MapMode.READ_WRITE, startByte + block * MAPPING_SIZE, sizeMapping);
			}

			// close previous mapping
			this.mappings = mappings;

			set0(oldSize, newSize);
		} catch (Throwable e) {
			try {
				throw e;
			} finally {
				IOUtil.closeAll(mappings);
			}
		}
	}

	@Override
	public void clear() {
		set0(0, length());
	}

	/**
	 * @return the location of the array disk
	 */
	public Path getLocation() {
		return location;
	}

	public long getSize() {
		return size;
	}

	public long getSizeBytes() {
		return size * 8L;
	}

}
