/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/
 * dictionary/impl/section/PFCDictionarySectionBig.java $ Revision: $Rev: 194 $
 * Last modified: $Date: 2013-03-04 21:30:01 +0000 (lun, 04 mar 2013) $ Last
 * modified by: $Author: mario.arias $ This library is free software; you can
 * redistribute it and/or modify it under the terms of the GNU Lesser General
 * Public License as published by the Free Software Foundation; version 3.0 of
 * the License. This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
 * General Public License for more details. You should have received a copy of
 * the GNU Lesser General Public License along with this library; if not, write
 * to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston,
 * MA 02110-1301 USA Contacting the authors: Mario Arias: mario.arias@deri.org
 * Javier D. Fernandez: jfergar@infor.uva.es Miguel A. Martinez-Prieto:
 * migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.dictionary.impl.section;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64Big;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionarySection;
import com.the_qa_company.qendpoint.core.exceptions.CRCException;
import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.Mutable;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.io.BigByteBuffer;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.ByteStringUtil;
import com.the_qa_company.qendpoint.core.util.string.CompactString;
import com.the_qa_company.qendpoint.core.util.string.ReplazableString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Iterator;

/**
 * Implementation of Plain Front Coding that divides the data in different
 * arrays, therefore overcoming Java's limitation of 2Gb for a single array. It
 * allows loading much bigger files, but waste some memory in pointers to the
 * blocks and some CPU to locate the array at search time.
 *
 * @author mario.arias, Lyudmila Balakireva
 */
public class PFCDictionarySectionBig implements DictionarySectionPrivate {
	private static final Logger log = LoggerFactory.getLogger(PFCDictionarySectionBig.class);

	public static final int TYPE_INDEX = 2;
	public static final int DEFAULT_BLOCK_SIZE = 16;
	public static final int BLOCK_PER_BUFFER = 1000000;

	BigByteBuffer[] data = new BigByteBuffer[0];
	long[] posFirst = new long[0];
	protected SequenceLog64Big blocks = new SequenceLog64Big();
	protected int blocksize;
	protected long numstrings;
	protected long size;
	static int filecounter = 0;

	public PFCDictionarySectionBig(HDTOptions spec) {
		this.blocksize = (int) spec.getInt("pfc.blocksize");
		if (blocksize == 0) {
			blocksize = DEFAULT_BLOCK_SIZE;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * hdt.dictionary.DictionarySection#load(hdt.dictionary.DictionarySection)
	 */
	@Override
	public void load(TempDictionarySection other, ProgressListener listener) {
		this.blocks = new SequenceLog64Big(BitUtil.log2(other.size()), other.getNumberOfElements() / blocksize);
		Iterator<? extends CharSequence> it = other.getSortedEntries();
		this.load(it, other.getNumberOfElements(), listener);

	}

	@Override
	public void load(Iterator<? extends CharSequence> it, long numentries, ProgressListener listener) {

		this.blocks = new SequenceLog64Big(64, numentries / blocksize);
		this.numstrings = 0;

		filecounter++;
		File file;

		try {
			file = File.createTempFile("test", ".tmp");
		} catch (IOException e) {
			throw new RuntimeException("Error creating temporary file.", e);
		}

		long byteoutsize = 0;
		ByteArrayOutputStream byteOut = new ByteArrayOutputStream(16 * 1024);
		ByteString previousStr = null;

		try {
			try {
				try (FileOutputStream out = new FileOutputStream(file)) {
					while (it.hasNext()) {
						ByteString str = ByteString.of(it.next());

						if (numstrings % blocksize == 0) {
							// Add new block pointer
							byteOut.flush();
							byteoutsize = byteoutsize + byteOut.size();

							blocks.append(byteoutsize);
							byteOut.writeTo(out);
							byteOut.reset();
							// Copy full string
							ByteStringUtil.append(byteOut, str, 0);

						} else {
							// Find common part.
							int delta = ByteStringUtil.longestCommonPrefix(previousStr, str);
							// Write Delta in VByte
							VByte.encode(byteOut, delta);
							// Write remaining
							ByteStringUtil.append(byteOut, str, delta);

						}
						byteOut.write(0); // End of string

						numstrings++;

						previousStr = str;

					}

					// Ending block pointer.
					byteOut.flush();
					byteoutsize = byteoutsize + byteOut.size();

					// blocks.append(byteOut.size());
					blocks.append(byteoutsize);
					// Trim text/blocks
					blocks.aggressiveTrimToSize();

					byteOut.flush();
					byteOut.writeTo(out);
				}

				try (InputStream in = new FileInputStream(file)) {
					// Read block by block
					// Read packed data

					int block = 0;
					int buffer = 0;
					long bytePos = 0;
					long numBlocks = blocks.getNumberOfElements();
					// System.out.println("numblocks:"+numBlocks);

					long numBuffers;
					if (numBlocks > 0) {
						// non empty section
						numBuffers = 1 + numBlocks / BLOCK_PER_BUFFER;
					} else {
						// else empty section then it's zero
						numBuffers = 0;
					}
					data = new BigByteBuffer[(int) numBuffers];
					posFirst = new long[(int) numBuffers];

					while (block < numBlocks - 1) {
						long nextBlock = Math.min(numBlocks - 1, block + BLOCK_PER_BUFFER);
						long nextBytePos = blocks.get(nextBlock);

						// System.out.println("Loading block: "+i+" from
						// "+previous+" to
						// "+ current+" of size "+ (current-previous));
						BigByteBuffer bigByteBuffer = BigByteBuffer.allocate(nextBytePos - bytePos);
						bigByteBuffer.readStream(in, 0, bigByteBuffer.size(), listener);
						data[buffer] = bigByteBuffer;

						posFirst[buffer] = bytePos;

						bytePos = nextBytePos;
						block += BLOCK_PER_BUFFER;
						buffer++;
					}
				}
			} finally {
				Files.delete(file.toPath());
			}
		} catch (IOException e) {
			throw new RuntimeException("Can't load dictionary.", e);
		}
	}

	/**
	 * Locate the block of a string doing binary search.
	 */
	protected long locateBlock(ByteString str) {
		long low = 0;
		long high = blocks.getNumberOfElements() - 1;
		long max = high;

		while (low <= high) {
			long mid = (low + high) >>> 1;

			int cmp;
			if (mid == max) {
				cmp = -1;
			} else {
				cmp = ByteStringUtil.strcmp(str, data[(int) (mid / BLOCK_PER_BUFFER)],
						blocks.get(mid) - posFirst[(int) (mid / BLOCK_PER_BUFFER)]);
				// System.out.println("Comparing against block: "+ mid + " which
				// is "+ ByteStringUtil.asString(data[mid], 0)+ " Result:
				// "+cmp);
			}

			if (cmp < 0) {
				high = mid - 1;
			} else if (cmp > 0) {
				low = mid + 1;
			} else {
				return mid; // key found
			}
		}
		return -(low + 1); // key not found.
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#locate(java.lang.CharSequence)
	 */
	@Override
	public long locate(CharSequence str) {
		ByteString bstr = ByteString.of(str);
		long blocknum = locateBlock(bstr);
		if (blocknum >= 0) {
			// Located exactly
			return (blocknum * blocksize) + 1;
		} else {
			// Not located exactly.
			blocknum = -blocknum - 2;

			if (blocknum >= 0) {
				long idblock = locateInBlock(blocknum, bstr);

				if (idblock != 0) {
					return (blocknum * blocksize) + idblock + 1;
				}
			}
		}

		// Not found
		return 0;
	}

	protected long locateInBlock(long blocknum, ByteString str) {

		ReplazableString tempString = new ReplazableString();

		Mutable<Long> delta = new Mutable<>(0L);
		long idInBlock = 0;
		int cshared = 0;

		BigByteBuffer block = data[(int) (blocknum / BLOCK_PER_BUFFER)];
		long pos = (blocks.get(blocknum) - posFirst[(int) (blocknum / BLOCK_PER_BUFFER)]);

		// Read the first string in the block
		int slen = (int) ByteStringUtil.strlen(block, pos);
		tempString.append(block, pos, slen);
		pos += slen + 1;
		idInBlock++;

		while ((idInBlock < blocksize) && (pos < block.size())) {
			// Decode prefix
			pos += VByte.decode(block, pos, delta);

			// Copy suffix
			slen = (int) ByteStringUtil.strlen(block, pos);
			tempString.replace(delta.getValue().intValue(), block, pos, slen);

			if (delta.getValue() >= cshared) {
				// Current delta value means that this string
				// has a larger long common prefix than the previous one
				cshared += ByteStringUtil.longestCommonPrefix(tempString, str, cshared);

				if ((cshared == str.length()) && (tempString.length() == str.length())) {
					return idInBlock;
				}
			} else {
				// We have less common characters than before,
				// this string is bigger that what we are looking for.
				// i.e. Not found.
				idInBlock = 0;
				break;
			}
			pos += slen + 1;
			idInBlock++;

		}

		// Not found
		if (pos == block.size() || idInBlock == blocksize) {
			idInBlock = 0;
		}

		return idInBlock;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#extract(int)
	 */
	@Override
	public CharSequence extract(long id) {

		if (id < 1 || id > numstrings) {
			return null;
		}

		// Locate block
		long blockid = (id - 1) / blocksize;
		long nstring = (id - 1) % blocksize;

		BigByteBuffer block = data[(int) (blockid / BLOCK_PER_BUFFER)];
		long pos = (blocks.get(blockid) - posFirst[(int) (blockid / BLOCK_PER_BUFFER)]);

		// Copy first string
		int len = (int) ByteStringUtil.strlen(block, pos);

		Mutable<Long> delta = new Mutable<>(0L);
		ReplazableString tempString = new ReplazableString();
		tempString.append(block, pos, len);

		// Copy strings until we find our's.
		for (long i = 0; i < nstring; i++) {
			pos += len + 1;
			pos += VByte.decode(block, pos, delta);
			len = (int) ByteStringUtil.strlen(block, pos);
			tempString.replace(delta.getValue().intValue(), block, pos, len);
		}
		return new CompactString(tempString).getDelayed();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#size()
	 */
	@Override
	public long size() {
		return size;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#getNumberOfElements()
	 */
	@Override
	public long getNumberOfElements() {
		return numstrings;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#getEntries()
	 */
	@Override
	public Iterator<CharSequence> getSortedEntries() {
		return new Iterator<>() {
			long pos;

			@Override
			public boolean hasNext() {
				return pos < getNumberOfElements();
			}

			@Override
			public CharSequence next() {
				// FIXME: It is more efficient to go through each block, each
				// entry.
				pos++;
				return extract(pos);
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#save(java.io.OutputStream,
	 * hdt.ProgressListener)
	 */
	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {

		CRCOutputStream out = new CRCOutputStream(output, new CRC8());
		out.write(TYPE_INDEX);
		VByte.encode(out, numstrings);

		long datasize = 0;

		for (BigByteBuffer bigByteBuffer : data) {
			datasize += bigByteBuffer.size();
		}
		VByte.encode(out, datasize);
		VByte.encode(out, blocksize);
		out.writeCRC();
		blocks.save(output, listener); // Write blocks directly to output, they
		// have their own CRC check.
		out.setCRC(new CRC32());
		for (BigByteBuffer datum : data) {
			datum.writeStream(out, 0, datum.size(), listener);
		}
		out.writeCRC();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#load(java.io.InputStream,
	 * hdt.ProgressListener)
	 */
	@Override
	public void load(InputStream input, ProgressListener listener) throws IOException {

		CRCInputStream in = new CRCInputStream(input, new CRC8());

		// Read type
		int type = in.read();
		if (type != TYPE_INDEX) {
			throw new IllegalFormatException(
					"Trying to read a DictionarySectionPFC from data that is not of the suitable type");
		}
		numstrings = VByte.decode(in);
		this.size = VByte.decode(in);
		blocksize = (int) VByte.decode(in);

		if (!in.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading Dictionary Section Plain Front Coding Header.");
		}

		// Load block pointers
		blocks = new SequenceLog64Big();
		blocks.load(input, listener);

		// Initialize global block array

		// Read block by block
		// Read packed data
		in.setCRC(new CRC32());

		int block = 0;
		int buffer = 0;
		long bytePos = 0;
		long numBlocks = blocks.getNumberOfElements();
		long numBuffers = 1 + numBlocks / BLOCK_PER_BUFFER;
		data = new BigByteBuffer[(int) numBuffers];
		posFirst = new long[(int) numBuffers];

		while (block < numBlocks - 1) {
			long nextBlock = Math.min(numBlocks - 1, block + BLOCK_PER_BUFFER);
			long nextBytePos = blocks.get(nextBlock);

			// System.out.println("Loading block: "+i+" from "+previous+" to "+
			// current+" of size "+ (current-previous));
			BigByteBuffer bigByteBuffer = BigByteBuffer.allocate(nextBytePos - bytePos);
			bigByteBuffer.readStream(in, 0, bigByteBuffer.size(), listener);
			data[buffer] = bigByteBuffer;

			posFirst[buffer] = bytePos;

			bytePos = nextBytePos;
			block += BLOCK_PER_BUFFER;
			buffer++;
		}

		if (!in.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading Dictionary Section Plain Front Coding Data.");
		}
	}

	@Override
	public void close() throws IOException {
		data = null;
		posFirst = null;
		// the constructor will not initialize blocks, so it can be null
		if (blocks != null) {
			blocks.close();
		}
		blocks = null;
	}
}
