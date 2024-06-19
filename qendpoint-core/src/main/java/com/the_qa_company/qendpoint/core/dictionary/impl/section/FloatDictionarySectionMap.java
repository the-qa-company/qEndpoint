package com.the_qa_company.qendpoint.core.dictionary.impl.section;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionarySection;
import com.the_qa_company.qendpoint.core.exceptions.CRCException;
import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;
import com.the_qa_company.qendpoint.core.util.io.BigMappedByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import com.the_qa_company.qendpoint.core.util.io.CountInputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.DoubleCompactString;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.Iterator;

public class FloatDictionarySectionMap implements DictionarySectionPrivate {
	public static final int TYPE_INDEX = 4;
	protected FileChannel ch;

	private final File f;
	private final long startOffset;
	private final long endOffset;
	BigMappedByteBuffer data;
	protected long numstrings;
	protected long size;

	public FloatDictionarySectionMap(CountInputStream input, File f) throws IOException {
		this.f = f;
		startOffset = input.getTotalBytes();

		CRCInputStream crcin = new CRCInputStream(input, new CRC8());

		// Read type
		int type = crcin.read();
		if (type != FloatDictionarySection.TYPE_INDEX) {
			throw new IllegalFormatException(
					"Trying to read a DictionarySectionPFC from data that is not of the suitable type");
		}

		// Read vars
		numstrings = VByte.decode(crcin);
		size = numstrings;

		if (!crcin.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading Dictionary Section Plain Front Coding Header.");
		}

		long base = input.getTotalBytes();
		IOUtil.skip(crcin, size * 8 + 4); // Including CRC32

		endOffset = input.getTotalBytes();

		// Read packed data
		ch = FileChannel.open(Paths.get(f.toString()));
		data = BigMappedByteBuffer.ofFileChannel(f.getAbsolutePath(), ch, FileChannel.MapMode.READ_ONLY, base,
				size * 8);
	}

	@Override
	public long locate(CharSequence s) {
		try {
			return IOUtil.binarySearch(data, ByteString.of(s).doubleValue(), 0, size) + 1;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private double extractDouble(long id) {
		if (id < 1 || id > size) {
			return 0;
		}

		try {
			return IOUtil.readLongDouble((id - 1) * 8, data);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public CharSequence extract(long id) {
		if (id < 1 || id > size) {
			return null;
		}

		return new DoubleCompactString(extractDouble(id));
	}

	@Override
	public long size() {
		return size * 8;
	}

	@Override
	public long getNumberOfElements() {
		return size;
	}

	@Override
	public Iterator<? extends CharSequence> getSortedEntries() {
		return new Iterator<>() {
			long pos;

			@Override
			public boolean hasNext() {
				return pos < getNumberOfElements();
			}

			@Override
			public CharSequence next() {
				return extract(++pos);
			}
		};
	}

	@Override
	public void load(TempDictionarySection other, ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public void load(Iterator<? extends CharSequence> it, long numentries, ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		InputStream in = new BufferedInputStream(new FileInputStream(f));
		IOUtil.skip(in, startOffset);
		IOUtil.copyStream(in, output, endOffset - startOffset);
		in.close();
	}

	@Override
	public void load(InputStream input, ProgressListener listener) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public void close() throws IOException {
		try {
			Closer.closeAll(data, ch);
		} finally {
			data = null;
			ch = null;
		}
	}
}
