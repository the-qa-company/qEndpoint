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
 * dennis.diefenbach@univ-st-etienne.fr
 */

package com.the_qa_company.qendpoint.core.compact.bitmap;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.exceptions.CRCException;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.disk.LargeLongArray;
import com.the_qa_company.qendpoint.core.util.disk.LongArray;
import com.the_qa_company.qendpoint.core.util.disk.LongArrayDisk;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;

/**
 * Keeps a bitmap as an array of long values (64 bit per word). The first bit is
 * the lowest significant bit of word zero, and so on. Can be stored on disk or
 * in memory without any limitation in size.
 *
 * @author mario.arias
 */
public class Bitmap64Big implements Closeable, ModifiableBitmap {
	/**
	 * create disk version bitmap
	 *
	 * @param location location
	 * @param nbits    number of bits
	 * @return bitmap
	 */
	public static Bitmap64Big disk(Path location, long nbits) {
		return new Bitmap64Big(new LongArrayDisk(location, numWords(nbits)));
	}

	/**
	 * create memory version bitmap
	 *
	 * @param nbits number of bits
	 * @return bitmap
	 */
	public static Bitmap64Big memory(long nbits) {
		return new Bitmap64Big(new LargeLongArray(IOUtil.createLargeArray(numWords(nbits))));
	}

	// Constants
	protected final static int LOGW = 6;
	protected final static int W = 64;

	// Variables
	protected long numbits;
	protected LongArray words;

	private final Closer closer;

	protected Bitmap64Big(LongArray words) {
		this.numbits = 0;
		this.words = words;
		closer = Closer.of();
		getCloser().with((Closeable) this::closeObject);
	}

	private void closeObject() throws IOException {
		Closer.closeAll(words);
	}

	/**
	 * Given a bit index, return word index containing it.
	 */
	protected static long wordIndex(long bitIndex) {
		return bitIndex >>> LOGW;
	}

	public static long numWords(long numbits) {
		if (numbits == 0) {
			return 0;
		}
		return ((numbits - 1) >>> LOGW) + 1;
	}

	public static long numBytes(long numbits) {
		return ((numbits - 1) >>> 3) + 1;
	}

	protected static int lastWordNumBits(long numbits) {
		if (numbits == 0) {
			return 0;
		}
		return (int) ((numbits - 1) % W) + 1; // +1 To have output in the range
												// 1-64, -1 to compensate.
	}

	protected final void ensureSize(long wordsRequired) throws IOException {
		if (words.length() < wordsRequired) {
			words.resize(Math.max(words.length() * 2, wordsRequired));
		}
	}

	public void trim(long numbits) {
		this.numbits = numbits;
	}

	public void trimToSize() {
		long wordNum = numWords(numbits);
		if (wordNum != words.length()) {
			try {
				words.resize(wordNum);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public boolean access(long bitIndex) {
		if (bitIndex < 0)
			throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);

		long wordIndex = wordIndex(bitIndex);
		if (wordIndex >= words.length()) {
			return false;
		}

		return (words.get(wordIndex) & (1L << bitIndex)) != 0;
	}

	@Override
	public long rank1(long pos) {
		throw new NotImplementedException();
	}

	@Override
	public long rank0(long pos) {
		throw new NotImplementedException();
	}

	@Override
	public long selectNext1(long fromIndex) {
		if (fromIndex < 0)
			throw new IndexOutOfBoundsException("fromIndex < 0: " + fromIndex);

		long wordIndex = wordIndex(fromIndex);
		if (wordIndex >= words.length())
			return -1;

		long word = words.get(wordIndex) & (~0L << fromIndex);

		while (true) {
			if (word != 0)
				return (wordIndex * W) + Long.numberOfTrailingZeros(word);
			if (++wordIndex == words.length())
				return -1;
			word = words.get(wordIndex);
		}
	}

	@Override
	public long select0(long n) {
		throw new NotImplementedException();
	}

	@Override
	public long select1(long n) {
		throw new NotImplementedException();
	}

	@Override
	public long countOnes() {
		// basic implementation
		if (words.length() == 0) {
			return 0;
		}
		long acc = 0;
		long end = wordIndex(numbits);
		if (end >= words.length()) {
			end = words.length() - 1;
		}
		for (long i = 0; i <= end; i++) {
			acc += Long.bitCount(words.get(i));
		}
		return acc;
	}

	@Override
	public long countZeros() {
		return words.length() * 64L - countOnes();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.compact.bitmap.ModifiableBitmap#append(boolean)
	 */
	public void append(boolean value) {
		this.set(numbits++, value);
	}

	public void set(long bitIndex, boolean value) {
		if (bitIndex < 0) {
			throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);
		}

		long wordIndex = wordIndex(bitIndex);
		try {
			ensureSize(wordIndex + 1);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		long wordText = words.get(wordIndex);
		long wordReplaced;
		if (value) {
			wordReplaced = wordText | (1L << bitIndex);
		} else {
			wordReplaced = wordText & ~(1L << bitIndex);
		}

		if (wordText != wordReplaced) {
			// we need to write something
			words.set(wordIndex, wordReplaced);
		}

		this.numbits = Math.max(this.numbits, bitIndex + 1);
	}

	@Override
	public String getType() {
		return HDTVocabulary.BITMAP_TYPE_PLAIN;
	}

	public long selectPrev1(long start) {
		throw new NotImplementedException();
	}

	public long getNumBits() {
		return numbits;
	}

	public long getSizeBytes() {
		return numWords(numbits) * 8;
	}

	public void save(OutputStream output, ProgressListener listener) throws IOException {
		CRCOutputStream out = new CRCOutputStream(output, new CRC8());

		// Write Type and Numbits
		out.write(BitmapFactory.TYPE_BITMAP_PLAIN);
		VByte.encode(out, numbits);

		// Write CRC
		out.writeCRC();

		// Setup new CRC
		out.setCRC(new CRC32());
		long numwords = numWords(numbits);
		for (long i = 0; i < numwords - 1; i++) {
			IOUtil.writeLong(out, words.get(i));
		}

		if (numwords > 0) {
			// Write only used bits from last entry (byte aligned, little
			// endian)
			long lastWordUsed = lastWordNumBits(numbits);
			BitUtil.writeLowerBitsByteAligned(words.get(numwords - 1), lastWordUsed, out);
		}

		out.writeCRC();
	}

	@Override
	public void load(InputStream input, ProgressListener listener) throws IOException {
		CRCInputStream in = new CRCInputStream(input, new CRC8());

		// Read type and numbits
		int type = in.read();
		if (type != BitmapFactoryImpl.TYPE_BITMAP_PLAIN) {
			throw new IllegalArgumentException("Trying to read BitmapPlain on a section that is not BitmapPlain");
		}
		numbits = VByte.decode(in);

		// Validate CRC
		if (!in.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading Bitmap64 header.");
		}

		// Setup Data CRC
		in.setCRC(new CRC32());

		// Read Words
		long numwords = numWords(numbits);
		ensureSize(numwords);
		for (long i = 0; i < numwords - 1; i++) {
			words.set(i, IOUtil.readLong(in));
		}

		if (numwords > 0) {
			// Read only used bits from last entry (byte aligned, little endian)
			long lastWordUsedBits = lastWordNumBits(numbits);
			words.set(numwords - 1, BitUtil.readLowerBitsByteAligned(lastWordUsedBits, in));
		}

		if (!in.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading Bitmap64 data.");
		}
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		for (long i = 0; i < numbits; i++) {
			str.append(access(i) ? '1' : '0');
		}
		return str.toString();
	}

	public long getRealSizeBytes() {
		return words.length() * 8;
	}

	/**
	 * @return bitmap closer object
	 */
	public Closer getCloser() {
		return closer;
	}

	@Override
	public void close() throws IOException {
		closer.close();
	}

	/**
	 * @return sync version of this bitmap
	 */
	public ModifiableBitmap asSync() {
		return SyncBitmap.of(this);
	}
}
