/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/compact
 * /sequence/SequenceLog64Map.java $ Revision: $Rev: 191 $ Last modified: $Date:
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

package com.the_qa_company.qendpoint.core.compact.sequence;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.exceptions.CRCException;
import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.storage.TempBuffIn;
import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.io.CloseMappedByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import com.the_qa_company.qendpoint.core.util.io.CountInputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * @author mario.arias
 */
public class SequenceLog64Map implements Sequence, Closeable {
	private static final byte W = 64;
	private static final long LONGS_PER_BUFFER = 128 * 1024 * 1024; // 128*8 =
	// 1Gb per
	// chunk.
	private CloseMappedByteBuffer[] buffers;
	private FileChannel ch;
	private final int numbits;
	private final long numentries;
	private long lastword;
	private final long numwords;

	public SequenceLog64Map(File f) throws IOException {
		// Read from the beginning of the file
		this(new CountInputStream(new TempBuffIn(new FileInputStream(f))), f, true);
	}

	public SequenceLog64Map(CountInputStream in, File f) throws IOException {
		this(in, f, false);
	}

	private SequenceLog64Map(CountInputStream in, File f, boolean closeInput) throws IOException {
		CRCInputStream crcin = new CRCInputStream(in, new CRC8());

		int type = crcin.read();
		if (type != SequenceFactory.TYPE_SEQLOG) {
			throw new IllegalFormatException("Trying to read a LogArray but the data is not LogArray");
		}
		numbits = crcin.read();
		numentries = VByte.decode(crcin);

		if (!crcin.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading LogArray64 header.");
		}

		if (numbits > 64) {
			throw new IllegalFormatException("LogArray64 cannot deal with more than 64bit per entry");
		}

		long base = in.getTotalBytes();

		numwords = SequenceLog64.numWordsFor(numbits, numentries);
		if (numwords > 0) {
			IOUtil.skip(in, (numwords - 1) * 8L);
			// Read only used bits from last entry (byte aligned, little endian)
			int lastWordUsed = SequenceLog64.lastWordNumBits(numbits, numentries);
			lastword = BitUtil.readLowerBitsByteAligned(lastWordUsed, in);
//			System.out.println("LastWord0: "+Long.toHexString(lastword));
		}
		IOUtil.skip(in, 4); // CRC

		mapFiles(f, base);

		if (closeInput) {
			in.close();
		}
	}

	public SequenceLog64Map(int numbits, long numentries, File f) throws IOException {
		this.numbits = numbits;
		this.numentries = numentries;
		this.numwords = SequenceLog64.numWordsFor(numbits, numentries);

		mapFiles(f, 0);
	}

	@Override
	public int sizeOf() {
		return numbits;
	}

	private void mapFiles(File f, long base) throws IOException {
		// Read packed data
		ch = FileChannel.open(Paths.get(f.toString()));
		long maxSize = base + SequenceLog64.numBytesFor(numbits, numentries);
		int buffer = 0;
		long block = 0;
		if (buffers != null) {
			IOUtil.closeAll(buffers);
		}
		buffers = new CloseMappedByteBuffer[(int) (1L + numwords / LONGS_PER_BUFFER)];
		while (block < numwords) {
			long current = base + buffer * 8L * LONGS_PER_BUFFER;
			long next = current + 8L * LONGS_PER_BUFFER;
			long length = Math.min(maxSize, next) - current;
//			System.out.println("Ini: "+current+ " Max: "+ next+ " Length: "+length);
			buffers[buffer] = IOUtil.mapChannel(f.getAbsolutePath(), ch, MapMode.READ_ONLY, current, length);
			buffers[buffer].order(ByteOrder.LITTLE_ENDIAN);

			block += LONGS_PER_BUFFER;
			buffer++;
		}

		// Read lastWord (it is special because it can be smaller than 8 bytes)
//		lastword = 0;
//		if(numwords>0) {
//			ByteBuffer lastBuffer = buffers[buffers.length-1];
//			int pos = lastBuffer.limit()-1;
//			int numBytesLast = SequenceLog64.lastWordNumBytes(numbits, numentries);
//			while(pos>=lastBuffer.limit()-numBytesLast) {
//				long read = (lastBuffer.get(pos) & 0xFFL);
//				System.out.println("Byte: "+pos+" / "+Long.toHexString(read));
//				lastword = (lastword << 8) | read;
//				pos--;
//			}
////			System.out.println("LastWord1: "+Long.toHexString(lastword)+" Bytes: "+numBytesLast);
//		}

		// FIXME: Bug in the previous code, find what because it should be more
		// efficient

		CountInputStream in = new CountInputStream(new TempBuffIn(new FileInputStream(f)));
		IOUtil.skip(in, base + ((numwords - 1) * 8L));
//		System.out.println("Last word starts at: "+in.getTotalBytes());
		// Read only used bits from last entry (byte aligned, little endian)
		int lastWordUsedBits = SequenceLog64.lastWordNumBits(numbits, numentries);
		lastword = BitUtil.readLowerBitsByteAligned(lastWordUsedBits, in);
//		System.out.println("Last word ends at: "+in.getTotalBytes());
//		System.out.println("LastWord2: "+Long.toHexString(lastword)+" Bits: "+lastWordUsedBits);
		in.close();
	}

	private long getWord(long w) {
		if (w == numwords - 1) {
			return lastword;
		}

		return buffers[(int) (w / LONGS_PER_BUFFER)].getLong((int) ((w % LONGS_PER_BUFFER) * 8));
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.array.Stream#get(long)
	 */
	@Override
	public long get(long index) {
		if (index < 0 || index >= numentries) {
			throw new IndexOutOfBoundsException(index + " < 0 || " + index + ">= " + numentries);
		}
		if (numbits == 0)
			return 0;

		long bitPos = index * numbits;
		long i = bitPos / W;
		int j = (int) (bitPos % W);
		long result;
		if (j + numbits <= W) {
			result = (getWord(i) << (W - j - numbits)) >>> (W - numbits);
		} else {
			result = getWord(i) >>> j;
			result = result | (getWord(i + 1) << ((W << 1) - j - numbits)) >>> (W - numbits);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.array.Stream#getNumberOfElements()
	 */
	@Override
	public long getNumberOfElements() {
		return numentries;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.array.Stream#save(java.io.OutputStream,
	 * hdt.ProgressListener)
	 */
	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		CRCOutputStream out = new CRCOutputStream(output, new CRC8());

		out.write(SequenceFactory.TYPE_SEQLOG);
		out.write(numbits);
		VByte.encode(out, numentries);

		out.writeCRC();

		out.setCRC(new CRC32());

		long numwords = SequenceLog64.numWordsFor(numbits, numentries);
		for (long i = 0; i < numwords - 1; i++) {
			IOUtil.writeLong(out, getWord(i));
		}

		if (numwords > 0) {
			// Write only used bits from last entry (byte aligned, little
			// endian)
			long lastWordUsedBits = SequenceLog64.lastWordNumBits(numbits, numentries);
			BitUtil.writeLowerBitsByteAligned(lastword, lastWordUsedBits, out);
		}

		out.writeCRC();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.array.Stream#size()
	 */
	@Override
	public long size() {
		return SequenceLog64.numBytesFor(numbits, numentries);
	}

	public int getNumBits() {
		return numbits;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.compact.array.Stream#getType()
	 */
	@Override
	public String getType() {
		return HDTVocabulary.SEQ_TYPE_LOG;
	}

	@Override
	public void add(Iterator<Long> elements) {
		throw new NotImplementedException();
	}

	@Override
	public void load(InputStream input, ProgressListener listener) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public void close() throws IOException {
		try {
			Closer.closeAll(buffers, ch);
		} finally {
			buffers = null;
		}
	}
}
