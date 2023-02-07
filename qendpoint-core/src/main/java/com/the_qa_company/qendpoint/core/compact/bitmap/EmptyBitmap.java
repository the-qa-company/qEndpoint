package com.the_qa_company.qendpoint.core.compact.bitmap;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * empty bitmap, act like a bitmap filled with 0, but isn't allocated on disk or
 * in memory, will throw a {@link NotImplementedException} if we try to add a
 * non 0 value
 */
public class EmptyBitmap implements ModifiableBitmap {
	/**
	 * create empty bitmap simulating a bitmap of a particular size
	 *
	 * @param size the size
	 * @return bitmap
	 */
	public static ModifiableBitmap of(long size) {
		return new EmptyBitmap(size);
	}

	private long size;

	private EmptyBitmap(long size) {
		this.size = size;
	}

	@Override
	public void append(boolean value) {
		set(size, value);
	}

	@Override
	public void set(long pos, boolean value) {
		if (value) {
			throw new NotImplementedException("true value in EmptyBitmap");
		}

		size = Math.max(size, pos);
	}

	@Override
	public boolean access(long pos) {
		return false;
	}

	@Override
	public long rank1(long pos) {
		return 0;
	}

	@Override
	public long rank0(long pos) {
		return pos;
	}

	@Override
	public long selectPrev1(long start) {
		return -1;
	}

	@Override
	public long selectNext1(long start) {
		return -1;
	}

	@Override
	public long select0(long n) {
		return n;
	}

	@Override
	public long select1(long n) {
		return -1;
	}

	@Override
	public long getNumBits() {
		return size;
	}

	@Override
	public long countOnes() {
		return 0;
	}

	@Override
	public long countZeros() {
		return size;
	}

	@Override
	public long getSizeBytes() {
		return 0;
	}

	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		CRCOutputStream out = new CRCOutputStream(output, new CRC8());

		// Write Type and Numbits
		out.write(BitmapFactory.TYPE_BITMAP_PLAIN);
		VByte.encode(out, 8L);

		// Write CRC
		out.writeCRC();

		// Setup new CRC
		out.setCRC(new CRC32());
		IOUtil.writeLong(out, 0);

		out.writeCRC();
	}

	@Override
	public void load(InputStream input, ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public String getType() {
		return HDTVocabulary.BITMAP_TYPE_PLAIN;
	}
}
