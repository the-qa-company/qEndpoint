package com.the_qa_company.qendpoint.core.compact.bitmap;

import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.io.CloseMappedByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Mapped {@link Bitmap} wrapper for the {@link ImmutableRoaringBitmap}, not compatible with {@link RoaringBitmap64}
 *
 * @author Antoine Willerval
 */
public class MappedRoaringBitmap implements SimpleBitmap, Closeable {
	private final CloseMappedByteBuffer buffer;
	private final ImmutableRoaringBitmap rbm;

	public MappedRoaringBitmap(CloseMappedByteBuffer buffer) {
		this.buffer = buffer;
		this.rbm = new ImmutableRoaringBitmap(buffer.getInternalBuffer());
	}

	public ImmutableRoaringBitmap getHandle() {
		return rbm;
	}

	@Override
	public boolean access(long position) {
		return rbm.contains((int) position);
	}

	@Override
	public long getNumBits() {
		return rbm.last();
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
	public String getType() {
		return HDTVocabulary.BITMAP_TYPE_ROARING;
	}

	@Override
	public long select1(long n) {
		long position = n - 1;
		if (position == -1)
			return -1;
		if (position < rbm.getLongCardinality()) {
			return rbm.select((int) position);
		} else {
			return rbm.select((int) rbm.getLongCardinality() - 1) + 1;
		}
	}

	@Override
	public long rank1(long position) {
		if (position >= 0)
			return rbm.rankLong((int) position);
		return 0;
	}

	@Override
	public long countOnes() {
		return rbm.getLongCardinality();
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

	@Override
	public void close() throws IOException {
		buffer.close();
	}
}
