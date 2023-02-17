/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/
 * dictionary/impl/section/PFCDictionarySection.java $ Revision: $Rev: 201 $
 * Last modified: $Date: 2013-04-17 23:40:20 +0100 (mié, 17 abr 2013) $ Last
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
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.ByteStringUtil;
import com.the_qa_company.qendpoint.core.util.string.CompactString;
import com.the_qa_company.qendpoint.core.util.string.ReplazableString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

/**
 * @author mario.arias
 */
public class PFCDictionarySection implements DictionarySectionPrivate {
	private static final Logger log = LoggerFactory.getLogger(PFCDictionarySection.class);

	public static final int TYPE_INDEX = 2;
	public static final int DEFAULT_BLOCK_SIZE = 16;

	// FIXME: Due to java array indexes being int, only 2GB can be addressed per
	// dictionary section.
	protected byte[] text = new byte[0]; // Encoded sequence
	protected int blocksize;
	protected int numstrings;
	protected SequenceLog64 blocks = new SequenceLog64();

	public PFCDictionarySection(HDTOptions spec) {
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
		this.blocks = new SequenceLog64(BitUtil.log2(other.size()), other.getNumberOfElements() / blocksize);
		Iterator<? extends CharSequence> it = other.getSortedEntries();
		this.load(it, other.getNumberOfElements(), listener);
	}

	public void load(PFCDictionarySectionBuilder builder) throws IOException {
		builder.finished();
		this.numstrings = builder.getNumstrings();
		this.text = builder.getText();
		this.blocks = builder.getBlocks();
		this.blocksize = builder.getBlocksize();
	}

	public void load(Iterator<? extends CharSequence> it, long numentries, ProgressListener listener) {
		this.blocks = new SequenceLog64(32, numentries / blocksize);
		this.numstrings = 0;

		ByteArrayOutputStream byteOut = new ByteArrayOutputStream(16 * 1024);

		CharSequence previousStr = null;

		try {
			while (it.hasNext()) {
				ByteString str = (ByteString) it.next();

				if (numstrings % blocksize == 0) {
					// Add new block pointer
					blocks.append(byteOut.size());

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
			blocks.append(byteOut.size());

			// Trim text/blocks
			blocks.aggressiveTrimToSize();

			byteOut.flush();
			text = byteOut.toByteArray();

			// DEBUG
			// dumpAll();
		} catch (IOException e) {
			log.error("Unexpected exception.", e);
		}
	}

	protected int locateBlock(ByteString str) {
		if (blocks.getNumberOfElements() == 0) {
			return -1;
		}

		int low = 0;
		int high = (int) blocks.getNumberOfElements() - 1;
		int max = high;

		while (low <= high) {
			int mid = (low + high) >>> 1;

			int cmp;
			if (mid == max) {
				cmp = -1;
			} else {
				int pos = (int) blocks.get(mid);
				cmp = ByteStringUtil.strcmp(str, text, pos);
//				System.out.println("Comparing against block: "+ mid + " which is "+ ByteStringUtil.asString(text, pos)+ " Result: "+cmp);
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
		if (text == null || blocks == null) {
			return 0;
		}

		// convert into bytestring to avoid bad comparison
		ByteString bstr = ByteString.of(str);

		int blocknum = locateBlock(bstr);
		if (blocknum >= 0) {
			// Located exactly
			return ((long) blocknum * blocksize) + 1;
		} else {
			// Not located exactly.
			blocknum = -blocknum - 2;

			if (blocknum >= 0) {
				int idblock = locateInBlock(blocknum, bstr);

				if (idblock != 0) {
					return ((long) blocknum * blocksize) + idblock + 1;
				}
			}
		}

		return 0;
	}

	public int locateInBlock(int block, ByteString str) {
		if (block >= blocks.getNumberOfElements()) {
			return 0;
		}

		int pos = (int) blocks.get(block);
		ReplazableString tempString = new ReplazableString();

		Mutable<Long> delta = new Mutable<>(0L);
		int idInBlock = 0;
		int cshared = 0;

//		dumpBlock(block);

		// Read the first string in the block
		int slen = ByteStringUtil.strlen(text, pos);
		tempString.append(text, pos, slen);
		pos += slen + 1;
		idInBlock++;

		while ((idInBlock < blocksize) && (pos < text.length)) {
			// Decode prefix
			pos += VByte.decode(text, pos, delta);

			// Copy suffix
			slen = ByteStringUtil.strlen(text, pos);
			tempString.replace(delta.getValue().intValue(), text, pos, slen);

			if (delta.getValue() >= cshared) {
				// Current delta value means that this string
				// has a larger long common prefix than the previous one
				cshared += ByteStringUtil.longestCommonPrefix(tempString, str, cshared);

				if ((cshared == str.length()) && (tempString.length() == str.length())) {
					break;
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

		if (pos >= text.length || idInBlock == blocksize) {
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
		if (text == null || blocks == null) {
			return null;
		}

		if (id < 1 || id > numstrings) {
			return null;
		}

		int block = (int) ((id - 1) / blocksize);
		int stringid = (int) ((id - 1) % blocksize);
		int pos = (int) blocks.get(block);
		int len = ByteStringUtil.strlen(text, pos);

		Mutable<Long> delta = new Mutable<>(0L);
		ReplazableString tempString = new ReplazableString();
		tempString.append(text, pos, len);

		for (int i = 0; i < stringid; i++) {
			pos += len + 1;
			pos += VByte.decode(text, pos, delta);
			len = ByteStringUtil.strlen(text, pos);
			tempString.replace(delta.getValue().intValue(), text, pos, len);
		}
		return new CompactString(tempString).getDelayed();
	}

//	private void dumpAll() {
//		for(int i=0;i<blocks.getNumberOfElements();i++) {
//			dumpBlock(i);
//		}
//	}
//
//	private void dumpBlock(int block) {
//		if(text==null || blocks==null || block>=blocks.getNumberOfElements()) {
//			return;
//		}
//
//		System.out.println("Dump block "+block);
//		ReplazableString tempString = new ReplazableString();
//		Mutable<Integer> delta = new Mutable<Integer>(0);
//		int idInBlock = 0;
//
//		int pos = (int)blocks.get(block);
//
//		// Copy first string
//		int len = ByteStringUtil.strlen(text, pos);
//		tempString.append(text, pos, len);
//		pos+=len+1;
//
//		System.out.println((block*blocksize+idInBlock)+ " ("+idInBlock+") => "+ tempString);
//		idInBlock++;
//
//		while( (idInBlock<blocksize) && (pos<text.length)) {
//			pos += VByte.decode(text, pos, delta);
//
//			len = ByteStringUtil.strlen(text, pos);
//			tempString.replace(delta.getValue(), text, pos, len);
//
//			System.out.println((block*blocksize+idInBlock)+ " ("+idInBlock+") => "+ tempString + " Delta="+delta.getValue()+ " Len="+len);
//
//			pos+=len+1;
//			idInBlock++;
//		}
//	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#size()
	 */
	@Override
	public long size() {
		return text.length + blocks.size();
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
			int id;
			int pos;
			final Mutable<Long> delta = new Mutable<>(0L);
			final ReplazableString tempString = new ReplazableString();

			@Override
			public boolean hasNext() {
				return id < getNumberOfElements();
			}

			@Override
			public CharSequence next() {
				int len;
				if ((id % blocksize) == 0) {
					len = ByteStringUtil.strlen(text, pos);
					tempString.replace(0, text, pos, len);
				} else {
					pos += VByte.decode(text, pos, delta);
					len = ByteStringUtil.strlen(text, pos);
					tempString.replace(delta.getValue().intValue(), text, pos, len);
				}
				pos += len + 1;
				id++;
				return new CompactString(tempString).getDelayed();
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
		VByte.encode(out, text.length);
		VByte.encode(out, blocksize);

		out.writeCRC();

		blocks.save(output, listener); // Write blocks directly to output, they
										// have their own CRC check.

		out.setCRC(new CRC32());
		IOUtil.writeBuffer(out, text, 0, text.length, listener);
		out.writeCRC();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#load(java.io.InputStream,
	 * hdt.ProgressListener)
	 */
	@SuppressWarnings("resource")
	@Override
	public void load(InputStream input, ProgressListener listener) throws IOException {
		CRCInputStream in = new CRCInputStream(input, new CRC8());

		// Read type
		int type = in.read();
		if (type != TYPE_INDEX) {
			throw new IllegalFormatException(
					"Trying to read a DictionarySectionPFC from data that is not of the suitable type");
		}

		// Read vars
		numstrings = (int) VByte.decode(in);
		long bytes = VByte.decode(in);
		blocksize = (int) VByte.decode(in);

		if (!in.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading Dictionary Section Plain Front Coding Header.");
		}

		if (bytes > Integer.MAX_VALUE) {
			input.reset();
			throw new IllegalArgumentException("This class cannot process files with a packed buffer bigger than 2GB");
		}

		// Read blocks
		blocks = new SequenceLog64();
		blocks.load(input, listener); // Read blocks from input, they have their
										// own CRC check.

		// Read packed data
		in.setCRC(new CRC32());
		text = IOUtil.readBuffer(in, (int) bytes, listener);
		if (!in.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading Dictionary Section Plain Front Coding Data.");
		}
	}

	@Override
	public void close() throws IOException {
		text = null;
		blocks.close();
		blocks = null;
	}
}
