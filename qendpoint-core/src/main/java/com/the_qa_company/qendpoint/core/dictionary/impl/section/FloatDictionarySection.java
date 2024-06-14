package com.the_qa_company.qendpoint.core.dictionary.impl.section;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionarySection;
import com.the_qa_company.qendpoint.core.exceptions.CRCException;
import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.unsafe.UnsafeLongArray;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.DoubleCompactString;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

public class FloatDictionarySection implements DictionarySectionPrivate {
	public static final int TYPE_INDEX = 4;
	public static final double EPSILON = Double.MIN_NORMAL * 10;

	UnsafeLongArray data;
	protected long size;

	public FloatDictionarySection(HDTOptions spec) {
	}


	public static int compareDoubleDelta(double d1, double d2) {
		double v = d1 - d2;

		if (v < -EPSILON) {
			return -1;
		}
		if (v > EPSILON) {
			return 1;
		}
		return 0;
	}

	@Override
	public long locate(CharSequence s) {
		return data.binarySearch(ByteString.of(s).doubleValue()) + 1;
	}

	private double extractDouble(long id) {
		if (id < 1 || id > size) {
			return 0;
		}

		// Locate block
		return data.getDouble(id - 1);
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
		return data.length() * data.sizeOf();
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
		Iterator<? extends CharSequence> it = other.getSortedEntries();
		this.load(it, other.getNumberOfElements(), listener);
	}

	@Override
	public void load(Iterator<? extends CharSequence> it, long numentries, ProgressListener listener) {
		data = UnsafeLongArray.allocate(numentries);

		while (it.hasNext()) {
			data.set(size++, ByteString.of(it.next()).doubleValue());
		}
	}

	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		CRCOutputStream out = new CRCOutputStream(output, new CRC8());

		out.write(TYPE_INDEX);
		VByte.encode(out, data.length());

		out.writeCRC();

		out.setCRC(new CRC32());
		data.write(out, size, 0);
		out.writeCRC();
	}

	@Override
	public void load(InputStream input, ProgressListener listener) throws IOException {
		CRCInputStream in = new CRCInputStream(input, new CRC8());

		// Read type
		int type = in.read();
		if (type != TYPE_INDEX) {
			throw new IllegalFormatException(
					"Trying to read a IntDictionarySection from data that is not of the suitable type");
		}

		this.size = VByte.decode(in);

		if (!in.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading Dictionary Section Plain Front Coding Header.");
		}

		in.setCRC(new CRC32());
		data = UnsafeLongArray.allocate(size);
		data.read(in, size, 0);

		if (!in.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading Dictionary Section data.");
		}
	}

	@Override
	public void close() throws IOException {
		Closer.closeAll(data);
	}
}