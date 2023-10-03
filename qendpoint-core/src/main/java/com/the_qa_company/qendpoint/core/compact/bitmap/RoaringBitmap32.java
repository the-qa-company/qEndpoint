package com.the_qa_company.qendpoint.core.compact.bitmap;

import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import org.roaringbitmap.RoaringBitmap;
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
public class RoaringBitmap32 implements SimpleModifiableBitmap {
	private final RoaringBitmap rbm;

	public RoaringBitmap32() {
		this.rbm = new RoaringBitmap();
	}

	public RoaringBitmap getHandle() {
		return rbm;
	}

	@Override
	public boolean access(long position) {
		if (position < 0 || position > Integer.MAX_VALUE) {
			return false;
		}
		return rbm.contains((int) position);
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
		assert position >= 0 && position < Integer.MAX_VALUE;
		if (value) {
			rbm.add((int) position);
		} else {
			rbm.remove((int) position);
		}
	}

	@Override
	public long select1(long n) {
		assert n >= 0 && n <= Integer.MAX_VALUE;
		int position = (int) (n - 1);
		if (position == -1)
			return -1;
		if (position < rbm.getLongCardinality()) {
			return rbm.select(position);
		} else {
			return rbm.select(rbm.getCardinality() - 1) + 1;
		}
	}

	@Override
	public long rank1(long position) {
		if (position >= 0 && position <= Integer.MAX_VALUE)
			return rbm.rankLong((int) position);
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
