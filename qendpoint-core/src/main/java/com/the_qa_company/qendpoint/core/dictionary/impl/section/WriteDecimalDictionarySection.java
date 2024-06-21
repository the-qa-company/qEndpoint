package com.the_qa_company.qendpoint.core.dictionary.impl.section;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64Big;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64BigDisk;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionarySection;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.unsafe.UnsafeLongArray;
import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.CountOutputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.listener.ListenerUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

public class WriteDecimalDictionarySection implements DictionarySectionPrivate {
	private final CloseSuppressPath tempFilename;
	private final CloseSuppressPath blockTempFilename;
	private SequenceLog64BigDisk blocks;
	private final int blocksize;
	private final int bufferSize;
	private long numberElements = 0;
	private long byteoutSize;
	private long size;
	private boolean created;

	public WriteDecimalDictionarySection(HDTOptions spec, Path filename, int bufferSize) {
		this.bufferSize = bufferSize;
		String fn = filename.getFileName().toString();
		tempFilename = CloseSuppressPath.of(filename.resolveSibling(fn + "_temp"));
		blockTempFilename = CloseSuppressPath.of(filename.resolveSibling(fn + "_tempblock"));
		blocksize = (int) spec.getInt("rpl.decsec.blocksize", DecimalDictionarySection.NUMBER_PER_BLOCK);
		if (blocksize <= 0) {
			throw new IllegalArgumentException("rpl.decsec.blocksize should be at least 1");
		}
		blocks = new SequenceLog64BigDisk(blockTempFilename.toAbsolutePath().toString(), 64, 1);
	}

	@Override
	public void load(TempDictionarySection other, ProgressListener plistener) {
		load(other.getSortedEntries(), other.getNumberOfElements(), plistener);
	}

	public WriteDictionarySectionAppender createAppender(long count, ProgressListener listener) throws IOException {
		blocks.close();
		Files.deleteIfExists(blockTempFilename);
		blocks = new SequenceLog64BigDisk(blockTempFilename.toAbsolutePath().toString(), 64, count / blocksize);
		return new WriteDictionarySectionAppender(count, listener);
	}

	@Override
	public void load(Iterator<? extends CharSequence> it, long count, ProgressListener plistener) {
		MultiThreadListener listener = ListenerUtil.multiThreadListener(plistener);
		long block = count < 10 ? 1 : count / 10;
		try {
			blocks.close();
			Files.deleteIfExists(blockTempFilename);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		blocks = new SequenceLog64BigDisk(blockTempFilename, 64, count / blocksize);
		long blockStart = 0;

		listener.notifyProgress(0, "Filling section");
		try (CountOutputStream out = new CountOutputStream(tempFilename.openOutputStream(bufferSize))) {
			CRCOutputStream crcout = new CRCOutputStream(out, new CRC32());
			byte[][] buffer = new byte[blocksize][];
			int[] bufferScales = new int[blocksize];

			while (it.hasNext()) {
				// read all the strings
				int allocated = 0;
				int maxSize = 0;
				do {
					BigDecimal dec = ByteString.of(it.next()).decimalValue();

					bufferScales[allocated] = dec.scale();
					byte[] d = dec.unscaledValue().toByteArray();
					buffer[allocated++] = d;
					if (maxSize < d.length)
						maxSize = d.length;
				} while (allocated < blocksize && it.hasNext());
				blocks.append(blockStart);

				VByte.encode(crcout, maxSize); // max buffer size
				for (int i = 0; i < allocated; i++) {
					VByte.encodeSigned(crcout, bufferScales[i]); // scale
					IOUtil.writeSizedBuffer(crcout, buffer[i], listener); // unscaled
					// value
				}

				numberElements += allocated;
				blockStart = out.getTotalBytes();
			}
			size = blockStart;
			byteoutSize = out.getTotalBytes();
			crcout.writeCRC();
		} catch (IOException e) {
			throw new RuntimeException("can't load section", e);
		}
		blocks.append(byteoutSize);
		// Trim text/blocks
		blocks.aggressiveTrimToSize();
		listener.notifyProgress(100, "Completed section filling");
		created = true;
	}

	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		CRCOutputStream out = new CRCOutputStream(output, new CRC8());

		out.write(DecimalDictionarySection.TYPE_INDEX);
		VByte.encode(out, numberElements);
		VByte.encode(out, size);
		VByte.encode(out, blocksize);

		out.writeCRC();

		if (created) {
			blocks.save(output, listener);
			// Write blocks data directly to output, the load was writing using
			// a
			// CRC check.
			Files.copy(tempFilename, output);
		} else {
			try (SequenceLog64Big longs = new SequenceLog64Big(1, 0, true)) {
				// save an empty one because we didn't ingest this section
				longs.save(output, listener);
			}
			out.setCRC(new CRC32());
			// write empty an empty data section because we don't have anything
			out.writeCRC();
		}
	}

	@Override
	public void load(InputStream input, ProgressListener listener) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public long locate(CharSequence s) {
		throw new NotImplementedException();
	}

	@Override
	public CharSequence extract(long pos) {
		throw new NotImplementedException();
	}

	@Override
	public long size() {
		return numberElements;
	}

	@Override
	public long getNumberOfElements() {
		return numberElements;
	}

	@Override
	public Iterator<? extends CharSequence> getSortedEntries() {
		throw new NotImplementedException();
	}

	@Override
	public void close() throws IOException {
		IOUtil.closeAll(blocks, tempFilename, blockTempFilename);
	}

	public class WriteDictionarySectionAppender implements Closeable {
		private final ProgressListener listener;
		private final long count;

		private final long block;
		private long blockStart;
		private final CountOutputStream out;
		long currentCount = 0;
		CRCOutputStream crcout;
		int allocated = 0;
		int maxSize = 0;
		long min = Long.MAX_VALUE;
		long max = Long.MIN_VALUE;
		byte[][] buffer = new byte[blocksize][];
		int[] bufferScales = new int[blocksize];

		public WriteDictionarySectionAppender(long count, ProgressListener listener) throws IOException {
			this.listener = ProgressListener.ofNullable(listener);
			this.count = count;
			this.block = count < 10 ? 1 : count / 10;
			out = new CountOutputStream(tempFilename.openOutputStream(bufferSize));
			crcout = new CRCOutputStream(out, new CRC32());
			resetCounter();
		}

		private void resetCounter() {
			allocated = 0;
			min = Long.MAX_VALUE;
			max = Long.MIN_VALUE;
			maxSize = 0;
		}

		public void append(ByteString str) throws IOException {
			BigDecimal dec = ByteString.of(str).decimalValue();

			bufferScales[allocated] = dec.scale();
			byte[] d = dec.unscaledValue().toByteArray();
			buffer[allocated++] = d;
			if (maxSize < d.length)
				maxSize = d.length;
			if (allocated < blocksize) {
				return; // no need to complete block
			}

			endBlock();
		}

		private void endBlock() throws IOException {
			if (allocated == 0) {
				return;
			}

			blocks.append(blockStart);

			VByte.encode(crcout, maxSize); // max buffer size
			for (int i = 0; i < allocated; i++) {
				VByte.encodeSigned(crcout, bufferScales[i]); // scale
				IOUtil.writeSizedBuffer(crcout, buffer[i], listener); // unscaled
				// value
			}

			blockStart = out.getTotalBytes();
			numberElements += allocated;

			listener.notifyProgress((float) (numberElements * 100 / count), "Filling section");

			resetCounter();
		}

		public long getNumberElements() {
			return numberElements;
		}

		@Override
		public void close() throws IOException {
			try {
				endBlock();
				byteoutSize = out.getTotalBytes();
				crcout.writeCRC();
				blocks.append(byteoutSize);
				size = byteoutSize;
				// Trim text/blocks
				blocks.aggressiveTrimToSize();
				listener.notifyProgress(100, "Completed section filling");
				created = true;

			} finally {
				IOUtil.closeObject(out);
			}
		}
	}
}
