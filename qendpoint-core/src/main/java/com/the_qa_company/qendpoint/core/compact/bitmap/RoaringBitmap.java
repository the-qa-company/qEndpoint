package com.the_qa_company.qendpoint.core.compact.bitmap;

import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import org.roaringbitmap.longlong.Roaring64Bitmap;

/**
 * {@link ModifiableBitmap} wrapper of the {@link Roaring64Bitmap} class, it
 * supports the {@link #set(long, boolean)}, {@link #access(long)},
 * {@link #append(boolean)} {@link #select1(long)}, {@link #rank1(long)},
 * {@link #rank0(long)} and {@link #countOnes()} methods.
 *
 * @author Antoine Willerval
 */
public class RoaringBitmap implements SimpleModifiableBitmap {
	private final Roaring64Bitmap rbm;

	public RoaringBitmap() {
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
		throw new NotImplementedException();
	}

	@Override
	public long getSizeBytes() {
		return 0;
	}

	@Override
	public String getType() {
		return HDTVocabulary.BITMAP_TYPE_ROAR;
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
		return rbm.select(n);
	}

	@Override
	public long rank1(long position) {
		return rbm.rankLong(position);
	}

	@Override
	public long countOnes() {
		return rbm.getLongCardinality();
	}

	@Override
	public void append(boolean value) {
		set(rbm.last() + 1, value);
	}
}
