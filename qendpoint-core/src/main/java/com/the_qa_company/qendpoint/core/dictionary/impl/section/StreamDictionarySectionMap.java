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
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;
import com.the_qa_company.qendpoint.core.util.io.BigMappedByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.BigMappedByteBufferInputStream;
import com.the_qa_company.qendpoint.core.util.io.CountInputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.ReplazableString;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.Iterator;

public class StreamDictionarySectionMap implements DictionarySectionPrivate, Closeable {

	private final BigMappedByteBuffer data;
	private final long numstrings;
	private final long bufferSize;
	private final CompressionType compressionType;

	private final File f;
	private final long startOffset;
	private final long endOffset;
	protected FileChannel ch;

	public StreamDictionarySectionMap(CountInputStream input, File f) throws IOException {
		this.f = f;
		startOffset = input.getTotalBytes();

		CRCInputStream crcin = new CRCInputStream(input, new CRC8());

		// Read type
		int type = crcin.read();
		if (type != StreamDictionarySection.TYPE_INDEX) {
			throw new IllegalFormatException(
					"Trying to read a StreamDictionarySection from data that is not of the suitable type");
		}
		// Read vars
		numstrings = VByte.decode(crcin);
		bufferSize = VByte.decode(crcin);

		String compressionFormatName = IOUtil.readSizedString(crcin, ProgressListener.ignore());

		try {
			compressionType = CompressionType.valueOf(compressionFormatName);
		} catch (IllegalArgumentException e) {
			throw new IOException("can't find compression type implementation with name " + compressionFormatName, e);
		}

		if (!crcin.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading Dictionary Section Header.");
		}

		long base = input.getTotalBytes();
		IOUtil.skip(crcin, bufferSize + 4); // Including CRC32

		endOffset = input.getTotalBytes();

		// Read packed data
		ch = FileChannel.open(Paths.get(f.toString()));

		data = BigMappedByteBuffer.ofFileChannel(f.getAbsolutePath(), ch, FileChannel.MapMode.READ_ONLY, base,
				bufferSize);
		data.order(ByteOrder.LITTLE_ENDIAN); // why do we use that?
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
		try (InputStream in = new BufferedInputStream(new FileInputStream(f))) {
			IOUtil.skip(in, startOffset);
			IOUtil.copyStream(in, output, endOffset - startOffset);
		}

	}

	@Override
	public void load(InputStream input, ProgressListener listener) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public void close() throws IOException {
		try {
			data.clean();
		} finally {
			ch.close();
		}
	}

	private class StreamBufferReader extends FetcherIterator<ByteString> {
		long idx;

		final InputStream is;

		StreamBufferReader() {
			try {
				is = compressionType.decompress(new BigMappedByteBufferInputStream(data));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		ReplazableString current = new ReplazableString(32);

		@Override
		protected ByteString getNext() {
			if (idx >= numstrings) {
				return null;
			}

			try {
				int delta = (int) VByte.decode(is);
				current.replace2(is, delta);
				idx++;

				return current;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public boolean isIndexedSection() {
		return false;
	}
}
