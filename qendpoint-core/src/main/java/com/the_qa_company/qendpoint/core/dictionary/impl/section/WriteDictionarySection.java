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
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.CountOutputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.listener.ListenerUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.ByteStringUtil;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

/**
 * Implementation of {@link DictionarySectionPrivate} that write loaded
 * {@link TempDictionarySection} on disk before saving, reducing the size in
 * ram, iterator should be a byte string
 *
 * @author Antoine Willerval
 */
public class WriteDictionarySection implements DictionarySectionPrivate {
	private final CloseSuppressPath tempFilename;
	private final CloseSuppressPath blockTempFilename;
	private SequenceLog64BigDisk blocks;
	private final long blockSize;
	private final int bufferSize;
	private long numberElements = 0;
	private long byteoutSize;
	private boolean created;

	public WriteDictionarySection(HDTOptions spec, Path filename, int bufferSize) {
		this.bufferSize = bufferSize;
		String fn = filename.getFileName().toString();
		tempFilename = CloseSuppressPath.of(filename.resolveSibling(fn + "_temp"));
		blockTempFilename = CloseSuppressPath.of(filename.resolveSibling(fn + "_tempblock"));
		blockSize = spec.getInt("pfc.blocksize", PFCDictionarySection.DEFAULT_BLOCK_SIZE);
		if (blockSize < 0) {
			throw new IllegalArgumentException("negative pfc.blocksize");
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
		blocks = new SequenceLog64BigDisk(blockTempFilename.toAbsolutePath().toString(), 64, count / blockSize);
		return new WriteDictionarySectionAppender(count, listener);
	}

	@Override
	public void load(Iterator<? extends CharSequence> it, long count, ProgressListener plistener) {
		MultiThreadListener listener = ListenerUtil.multiThreadListener(plistener);
		long block = count < 10 ? 1 : count / 10;
		long currentCount = 0;
		try {
			blocks.close();
			Files.deleteIfExists(blockTempFilename);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		blocks = new SequenceLog64BigDisk(blockTempFilename.toAbsolutePath().toString(), 64, count / blockSize);

		listener.notifyProgress(0, "Filling section");
		try (CountOutputStream out = new CountOutputStream(tempFilename.openOutputStream(bufferSize))) {
			CRCOutputStream crcout = new CRCOutputStream(out, new CRC32());
			ByteString previousStr = null;
			for (; it.hasNext(); currentCount++) {
				ByteString str = (ByteString) (it.next());
				assert str != null;
				if (numberElements % blockSize == 0) {
					blocks.append(out.getTotalBytes());

					// Copy full string
					ByteStringUtil.append(crcout, str, 0);
				} else {
					// Find common part.
					int delta = ByteStringUtil.longestCommonPrefix(previousStr, str);
					// Write Delta in VByte
					VByte.encode(crcout, delta);
					// Write remaining
					ByteStringUtil.append(crcout, str, delta);
				}
				crcout.write(0);
				previousStr = str;
				numberElements++;
				if (currentCount % block == 0) {
					listener.notifyProgress((float) (currentCount * 100 / count), "Filling section");
				}
			}

			byteoutSize = out.getTotalBytes();
			crcout.writeCRC();
		} catch (IOException e) {
			throw new RuntimeException("can't load section", e);
		}
		blocks.append(byteoutSize);
		// Trim text/blocks
		blocks.aggressiveTrimToSize();
		if (numberElements % 100_000 == 0) {
			listener.notifyProgress(100, "Completed section filling");
		}
		created = true;
	}

	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		CRCOutputStream out = new CRCOutputStream(output, new CRC8());
		out.write(PFCDictionarySection.TYPE_INDEX);
		VByte.encode(out, numberElements);

		VByte.encode(out, byteoutSize);
		VByte.encode(out, blockSize);
		out.writeCRC();
		// Write blocks directly to output, they have their own CRC check.
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
		private final CountOutputStream out;
		long currentCount = 0;
		CRCOutputStream crcout;
		ByteString previousStr = null;

		public WriteDictionarySectionAppender(long count, ProgressListener listener) throws IOException {
			this.listener = ProgressListener.ofNullable(listener);
			this.count = count;
			this.block = count < 10 ? 1 : count / 10;
			out = new CountOutputStream(tempFilename.openOutputStream(bufferSize));
			crcout = new CRCOutputStream(out, new CRC32());
		}

		public void append(ByteString str) throws IOException {
			assert str != null;
			if (numberElements % blockSize == 0) {
				blocks.append(out.getTotalBytes());

				// Copy full string
				ByteStringUtil.append(crcout, str, 0);
			} else {
				// Find common part.
				int delta = ByteStringUtil.longestCommonPrefix(previousStr, str);
				// Write Delta in VByte
				VByte.encode(crcout, delta);
				// Write remaining
				ByteStringUtil.append(crcout, str, delta);
			}
			crcout.write(0);
			previousStr = str;
			numberElements++;
			if (currentCount % block == 0) {
				listener.notifyProgress((float) (currentCount * 100 / count), "Filling section");
			}
			currentCount++;
		}

		public long getNumberElements() {
			return numberElements;
		}

		@Override
		public void close() throws IOException {
			try {
				byteoutSize = out.getTotalBytes();
				crcout.writeCRC();
				blocks.append(byteoutSize);
				// Trim text/blocks
				blocks.aggressiveTrimToSize();
				if (numberElements % 100_000 == 0) {
					listener.notifyProgress(100, "Completed section filling");
				}
				created = true;

			} finally {
				IOUtil.closeObject(out);
			}
		}
	}
}
