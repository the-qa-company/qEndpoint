package com.the_qa_company.qendpoint.core.dictionary.impl.section;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionarySection;
import com.the_qa_company.qendpoint.core.enums.CompressionType;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.CountOutputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.listener.ListenerUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.ByteStringUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

public class WriteStreamDictionarySection implements DictionarySectionPrivate {
	private final CloseSuppressPath tempFilename;
	private long numberElements = 0;
	private final int bufferSize;
	private long byteoutSize;
	private boolean created;
	private final CompressionType compressionType;

	public WriteStreamDictionarySection(HDTOptions spec, Path filename, int bufferSize) {
		this.bufferSize = bufferSize;
		String fn = filename.getFileName().toString();
		tempFilename = CloseSuppressPath.of(filename.resolveSibling(fn + "_temp"));
		compressionType = CompressionType.findOptionVal(spec.get(HDTOptionsKeys.DISK_COMPRESSION_KEY));
	}

	@Override
	public void load(TempDictionarySection other, ProgressListener plistener) {
		load(other.getSortedEntries(), other.getNumberOfElements(), plistener);
	}

	@Override
	public void load(Iterator<? extends CharSequence> it, long count, ProgressListener plistener) {
		MultiThreadListener listener = ListenerUtil.multiThreadListener(plistener);
		long block = count < 10 ? 1 : count / 10;
		long currentCount = 0;

		listener.notifyProgress(0, "Filling section");
		try (CountOutputStream countOut = new CountOutputStream(tempFilename.openOutputStream(bufferSize));
		     OutputStream out = compressionType.compress(countOut)) {
			CRCOutputStream crcout = new CRCOutputStream(out, new CRC32());
			ByteString previousStr = ByteString.empty();
			for (; it.hasNext(); currentCount++) {
				ByteString str = (ByteString) (it.next());
				assert str != null;
				// Find common part.
				int delta = ByteStringUtil.longestCommonPrefix(previousStr, str);
				// Write Delta in VByte
				VByte.encode(crcout, delta);
				// Write remaining
				ByteStringUtil.append(crcout, str, delta);
				crcout.write(0);
				previousStr = str;
				numberElements++;
				if (currentCount % block == 0) {
					listener.notifyProgress((float) (currentCount * 100 / count), "Filling section");
				}
			}

			byteoutSize = countOut.getTotalBytes();
			crcout.writeCRC();
		} catch (IOException e) {
			throw new RuntimeException("can't load section", e);
		}
		if (numberElements % 100_000 == 0) {
			listener.notifyProgress(100, "Completed section filling");
		}
		created = true;
	}

	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		CRCOutputStream out = new CRCOutputStream(output, new CRC8());
		out.write(StreamDictionarySection.TYPE_INDEX);
		VByte.encode(out, numberElements);
		VByte.encode(out, byteoutSize);
		IOUtil.writeSizedString(out, compressionType.name(), listener);
		out.writeCRC();
		if (created) {
			// Write blocks directly to output, they have their own CRC check.
			// we keep the stream compressed
			Files.copy(tempFilename, output);
		} else {
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
		return byteoutSize;
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
		IOUtil.closeAll(tempFilename);
	}
}
