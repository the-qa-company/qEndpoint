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
public class EmptyBitmap implements ModifiableBitmap, ModifiableMultiLayerBitmap {
	/**
	 * create empty bitmap simulating a bitmap of a particular size
	 *
	 * @param size the size
	 * @return bitmap
	 */
	public static EmptyBitmap of(long size) {
		return new EmptyBitmap(size, 0);
	}
	/**
	 * create empty bitmap simulating a bitmap of a particular size
	 *
	 * @param size the size
	 * @param layers layers
	 * @return bitmap
	 */
	public static EmptyBitmap of(long size, long layers) {
		return new EmptyBitmap(size, layers);
	}

	private long size;
	private final long layers;

	private EmptyBitmap(long size, long layers) {
		this.size = size;
		this.layers = layers;
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
	public boolean access(long layer, long position) {
		return false;
	}

	@Override
	public long rank1(long layer, long position) {
		return rank1(position);
	}

	@Override
	public long rank0(long layer, long position) {
		return rank0(position);
	}

	@Override
	public long selectPrev1(long layer, long start) {
		return selectPrev1(start);
	}

	@Override
	public long selectNext1(long layer, long start) {
		return selectNext1(start);
	}

	@Override
	public long select0(long layer, long n) {
		return select0(n);
	}

	@Override
	public long select1(long layer, long n) {
		return select1(n);
	}

	@Override
	public long getNumBits() {
		return size;
	}

	@Override
	public long countOnes(long layer) {
		return countOnes();
	}

	@Override
	public long countZeros(long layer) {
		return countZeros();
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
		return layers == 0 ? HDTVocabulary.BITMAP_TYPE_PLAIN : HDTVocabulary.BITMAP_TYPE_ROARING_MULTI;
	}

	@Override
	public long getLayersCount() {
		return layers;
	}

	@Override
	public void set(long layer, long position, boolean value) {
		set(position, value);
	}
}
