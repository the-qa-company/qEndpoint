/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/compact
 * /bitmap/Bitmap64.java $ Revision: $Rev: 191 $ Last modified: $Date:
 * 2013-03-03 11:41:43 +0000 (dom, 03 mar 2013) $ Last modified by: $Author:
 * mario.arias $ This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License as
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

package com.the_qa_company.qendpoint.core.compact.bitmap;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

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
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

/**
 * Keeps a bitmap as an array of long values (64 bit per word). The first bit is
 * the lowest significative bit of word zero, and so on.
 *
 * @author mario.arias
 * @deprecated use {@link Bitmap64Big#memory(long)} instead
 */
@Deprecated
public class Bitmap64 implements ModifiableBitmap {

	// Constants
	protected final static int LOGW = 6;
	protected final static int W = 64;

	// Variables
	protected long numbits;
	protected long[] words;

	public Bitmap64() {
		this(W);
	}

	public Bitmap64(long nbits) {
		this.numbits = 0;
		this.words = new long[(int) numWords(nbits)];
	}

	public Bitmap64(Bitmap64 other) {
		this(other, other.getNumBits());
	}

	public Bitmap64(Bitmap64 other, long numbits) {
		this.numbits = numbits;
		this.words = new long[(int) numWords(numbits)];
		if (this.words.length > 0) {
			this.words[this.words.length - 1] = 0;
		}
		System.arraycopy(other.words, 0, this.words, 0, words.length);
	}

	/**
	 * Given a bit index, return word index containing it.
	 */
	protected static int wordIndex(long bitIndex) {
		return (int) (bitIndex >>> LOGW);
	}

	public static long numWords(long numbits) {
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

	protected final void ensureSize(int wordsRequired) {
		if (words.length < wordsRequired) {
			long[] newWords = new long[Math.max(words.length * 2, wordsRequired)];
			System.arraycopy(words, 0, newWords, 0, Math.min(words.length, newWords.length));
			words = newWords;
		}
	}

	public void trim(long numbits) {
		this.numbits = numbits;
	}

	public void trimToSize() {
		int wordNum = (int) numWords(numbits);
		if (wordNum != words.length) {
			words = Arrays.copyOf(words, wordNum);
		}
	}

	@Override
	public boolean access(long bitIndex) {
		if (bitIndex < 0)
			throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);

		int wordIndex = wordIndex(bitIndex);
		if (wordIndex >= words.length) {
			return false;
		}

		return (words[wordIndex] & (1L << bitIndex)) != 0;
	}

	@Override
	public long rank1(long pos) {
		throw new NotImplementedException();
	}

	@Override
	public long rank0(long pos) {
		throw new NotImplementedException();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.compact.bitmap.ModifiableBitmap#append(boolean)
	 */
	@Override
	public void append(boolean value) {
		this.set(numbits++, value);
	}

	@Override
	public void set(long bitIndex, boolean value) {
		if (bitIndex < 0)
			throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);

		int wordIndex = wordIndex(bitIndex);
		ensureSize(wordIndex + 1);

		if (value) {
			words[wordIndex] |= (1L << bitIndex);
		} else {
			words[wordIndex] &= ~(1L << bitIndex);
		}

		this.numbits = Math.max(this.numbits, bitIndex + 1);
	}

	@Override
	public long selectPrev1(long start) {
		throw new NotImplementedException();
	}

	@Override
	public long selectNext1(long fromIndex) {
		if (fromIndex < 0)
			throw new IndexOutOfBoundsException("fromIndex < 0: " + fromIndex);

		int wordIndex = wordIndex(fromIndex);
		if (wordIndex >= words.length)
			return -1;

		long word = words[wordIndex] & (~0L << fromIndex);

		while (true) {
			if (word != 0)
				return ((long) wordIndex * W) + Long.numberOfTrailingZeros(word);
			if (++wordIndex == words.length)
				return -1;
			word = words[wordIndex];
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

	public long getWord(int word) {
		return words[word];
	}

	@Override
	public long getNumBits() {
		return numbits;
	}

	@Override
	public long countOnes() {
		if (words.length == 0)
			return 0;
		long acc = 0;
		int end = wordIndex(numbits);
		if (end >= words.length) {
			end = words.length - 1;
		}
		for (int i = 0; i <= end; i++)
			acc += Long.bitCount(words[i]);
		return acc;
	}

	@Override
	public long countZeros() {
		return words.length * 64L - countOnes();
	}

	@Override
	public long getSizeBytes() {
		return numWords(numbits) * 8;
	}

	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		CRCOutputStream out = new CRCOutputStream(output, new CRC8());

		// Write Type and Numbits
		out.write(BitmapFactory.TYPE_BITMAP_PLAIN);
		VByte.encode(out, numbits);

		// Write CRC
		out.writeCRC();

		// Setup new CRC
		out.setCRC(new CRC32());
		int numwords = (int) numWords(numbits);
		for (int i = 0; i < numwords - 1; i++) {
			IOUtil.writeLong(out, words[i]);
		}

		if (numwords > 0) {
			// Write only used bits from last entry (byte aligned, little
			// endian)
			int lastWordUsed = lastWordNumBits(numbits);
			BitUtil.writeLowerBitsByteAligned(words[numwords - 1], lastWordUsed, out);
		}

		out.writeCRC();
	}

	@Override
	@SuppressWarnings("resource")
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
		int numwords = (int) numWords(numbits);
		words = new long[numwords];
		for (int i = 0; i < numwords - 1; i++) {
			words[i] = IOUtil.readLong(in);
		}

		if (numwords > 0) {
			// Read only used bits from last entry (byte aligned, little endian)
			int lastWordUsedBits = lastWordNumBits(numbits);
			words[numwords - 1] = BitUtil.readLowerBitsByteAligned(lastWordUsedBits, in);
		}

		if (!in.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading Bitmap64 data.");
		}
	}

	@Override
	public String getType() {
		return HDTVocabulary.BITMAP_TYPE_PLAIN;
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
		return words.length * 8L;
	}
}
