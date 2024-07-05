package com.the_qa_company.qendpoint.core.dictionary.impl.section;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionType;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionarySection;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.listener.ListenerUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

public class WriteFloatDictionarySection implements DictionarySectionPrivate {
	private final CloseSuppressPath tempFilename;
	private long size;
	private boolean created;
	private final int bufferSize;

	public WriteFloatDictionarySection(HDTOptions spec, Path filename, int bufferSize) {
		this.bufferSize = bufferSize;
		String fn = filename.getFileName().toString();
		tempFilename = CloseSuppressPath.of(filename.resolveSibling(fn + "_temp"));
	}

	@Override
	public void load(TempDictionarySection other, ProgressListener plistener) {
		load(other.getSortedEntries(), other.getNumberOfElements(), plistener);
	}

	public WriteDictionarySectionAppender createAppender(long count, ProgressListener listener) throws IOException {
		return new WriteDictionarySectionAppender(count, listener);
	}

	@Override
	public void load(Iterator<? extends CharSequence> it, long count, ProgressListener plistener) {
		MultiThreadListener listener = ListenerUtil.multiThreadListener(plistener);

		listener.notifyProgress(0, "Filling section");
		long delta = count < 10 ? 1 : count / 10;
		try (OutputStream out = tempFilename.openOutputStream(bufferSize)) {
			CRCOutputStream crcout = new CRCOutputStream(out, new CRC32());
			long size = 0;
			while (it.hasNext()) {
				if (size++ % delta == 0) {
					listener.notifyProgress((float) (size * 100 / count), "Filling section");
				}
				IOUtil.writeLong(crcout, Double.doubleToLongBits(ByteString.of(it.next()).doubleValue()));
			}

			this.size = size;
			crcout.writeCRC();
		} catch (IOException e) {
			throw new RuntimeException("can't load section", e);
		}
		listener.notifyProgress(100, "Completed section filling");
		created = true;
	}

	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		CRCOutputStream out = new CRCOutputStream(output, new CRC8());

		out.write(FloatDictionarySection.TYPE_INDEX);
		VByte.encode(out, size);

		out.writeCRC();

		if (created) {
			Files.copy(tempFilename, output);
		} else {
			out.setCRC(new CRC32());
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
		return size;
	}

	@Override
	public long getNumberOfElements() {
		return size;
	}

	@Override
	public Iterator<? extends CharSequence> getSortedEntries() {
		throw new NotImplementedException();
	}

	@Override
	public void close() throws IOException {
		IOUtil.closeAll(tempFilename);
	}

	@Override
	public DictionarySectionType getSectionType() {
		return DictionarySectionType.FLOAT;
	}

	public class WriteDictionarySectionAppender implements Closeable {
		private final ProgressListener listener;
		private final long count;
		private final long block;
		private long numberElements;

		private final CRCOutputStream crcout;

		public WriteDictionarySectionAppender(long count, ProgressListener listener) throws IOException {
			this.listener = ProgressListener.ofNullable(listener);
			this.count = count;
			this.block = count < 10 ? 1 : count / 10;
			crcout = new CRCOutputStream(tempFilename.openOutputStream(bufferSize), new CRC32());
		}

		public void append(ByteString str) throws IOException {
			IOUtil.writeLong(crcout, Double.doubleToLongBits(str.doubleValue()));
			if (size++ % block == 0) {
				listener.notifyProgress((float) (size * 100 / count), "Filling section");
			}
		}

		public long getNumberElements() {
			return size;
		}

		@Override
		public void close() throws IOException {
			try {
				crcout.writeCRC();
				listener.notifyProgress(100, "Completed section filling");
				created = true;
			} finally {
				IOUtil.closeObject(crcout);
			}
		}
	}
}
