package com.the_qa_company.qendpoint.core.compact.bitmap;

import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * {@link ModifiableBitmap} wrapper of the {@link Roaring64Bitmap} class, it
 * supports the {@link #set(long, boolean)}, {@link #access(long)},
 * {@link #append(boolean)} {@link #select1(long)}, {@link #rank1(long)},
 * {@link #rank0(long)} and {@link #countOnes()} methods.
 *
 * @author Antoine Willerval
 */
public class RoaringBitmap64 implements SimpleModifiableBitmap {
	private final Roaring64Bitmap rbm;

	public RoaringBitmap64() {
		this.rbm = Roaring64Bitmap.bitmapOf();
	}

	public Roaring64Bitmap getHandle() {
		return rbm;
	}

	@Override
	public boolean access(long position) {
		return rbm.contains(position);
	}

	@Override
	public long getNumBits() {
		return rbm.getLongCardinality();
	}

	@Override
	public long getSizeBytes() {
		return rbm.serializedSizeInBytes() + 8;
	}

	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		long size = rbm.serializedSizeInBytes();
		IOUtil.writeLong(output, size);
		rbm.serialize(new DataOutputStream(output));
	}

	@Override
	public void load(InputStream input, ProgressListener listener) throws IOException {
		IOUtil.readLong(input); // ignored
		rbm.deserialize(new DataInputStream(input));
	}

	@Override
	public String getType() {
		return HDTVocabulary.BITMAP_TYPE_ROARING;
	}

	@Override
	public void set(long position, boolean value) {
		if (value) {
			rbm.addLong(position);
		} else {
			rbm.removeLong(position);
		}
	}

	@Override
	public long select1(long n) {
		long position = n - 1;
		if (position == -1)
			return -1;
		if (position < rbm.getLongCardinality()) {
			return rbm.select(position);
		} else {
			return rbm.select(rbm.getLongCardinality() - 1) + 1;
		}
	}

	@Override
	public long rank1(long position) {
		if (position >= 0)
			return rbm.rankLong(position);
		return 0;
	}

	@Override
	public long countOnes() {
		return rbm.getLongCardinality();
	}

	@Override
	public void append(boolean value) {
		set(rbm.last() + 1, value);
	}

	@Override
	public long selectPrev1(long start) {
		return select1(rank1(start));
	}

	@Override
	public long selectNext1(long start) {
		long pos = rank1(start - 1);
		if (pos < rbm.getLongCardinality())
			return select1(pos + 1);
		return -1;
	}
}
