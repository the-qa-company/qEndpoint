package com.the_qa_company.qendpoint.core.dictionary.impl.section;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionarySection;
import com.the_qa_company.qendpoint.core.enums.CompressionType;
import com.the_qa_company.qendpoint.core.exceptions.CRCException;
import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.iterator.utils.FetcherIterator;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.util.Mutable;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.io.BigByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.ReplazableString;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

public class StreamDictionarySection implements DictionarySectionPrivate, Closeable {
	public static final int TYPE_INDEX = 0x30;
	public static final int STREAM_SECTION_END_COOKIE = 0x48535324;
	BigByteBuffer data = BigByteBuffer.allocate(0);
	private long numstrings;
	private long bufferSize;
	private CompressionType compressionType = CompressionType.NONE;

	public StreamDictionarySection(HDTOptions spec) {
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
		return bufferSize;
	}

	@Override
	public long getNumberOfElements() {
		return numstrings;
	}

	@Override
	public Iterator<? extends CharSequence> getSortedEntries() {
		return new StreamBufferReader();
	}

	@Override
	public void load(TempDictionarySection other, ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public void load(Iterator<? extends CharSequence> it, long count, ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		CRCOutputStream out = new CRCOutputStream(output, new CRC8());

		out.write(TYPE_INDEX);
		VByte.encode(out, numstrings);
		VByte.encode(out, bufferSize);
		IOUtil.writeSizedString(out, compressionType.name(), listener);
		out.writeCRC();

		out.setCRC(new CRC32());
		data.writeStream(out, 0, bufferSize, listener);
		out.writeCRC();
		IOUtil.writeInt(out, STREAM_SECTION_END_COOKIE);
	}

	@Override
	public void load(InputStream input, ProgressListener listener) throws IOException {
		CRCInputStream in = new CRCInputStream(input, new CRC8());

		// Read type
		int type = in.read();
		if (type != TYPE_INDEX) {
			throw new IllegalFormatException(
					"Trying to read a StreamDictionarySection from data that is not of the suitable type");
		}
		numstrings = VByte.decode(in);
		bufferSize = VByte.decode(in);

		String compressionFormatName = IOUtil.readSizedString(in, listener);

		try {
			compressionType = CompressionType.valueOf(compressionFormatName);
		} catch (IllegalArgumentException e) {
			throw new IOException("can't find compression type implementation with name " + compressionFormatName, e);
		}

		if (!in.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading Dictionary Section Plain Front Coding Header.");
		}

		// read packed data
		in.setCRC(new CRC32());
		data = BigByteBuffer.allocate(bufferSize);
		data.readStream(in, 0, bufferSize, listener);

		if (!in.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading Dictionary Section Plain Front Coding Data.");
		}

		int cookie = IOUtil.readInt(in);
		if (cookie != STREAM_SECTION_END_COOKIE) {
			throw new IOException("Can't read stream triples end cookie, found 0x" + Integer.toHexString(cookie));
		}
	}

	@Override
	public void close() throws IOException {
		// nothing to do
	}

	private class StreamBufferReader extends FetcherIterator<ByteString> {
		long idx;
		long offset;

		ReplazableString current = new ReplazableString(32);
		Mutable<Long> nextIdx = new Mutable<>(0L);

		@Override
		protected ByteString getNext() {
			if (idx >= numstrings) {
				return null;
			}

			offset += VByte.decode(data, offset, nextIdx);

			int delta = nextIdx.getValue().intValue();

			offset += current.replace2(data, offset, delta);
			idx++;

			return current;
		}
	}

	@Override
	public boolean isIndexedSection() {
		return false;
	}
}
