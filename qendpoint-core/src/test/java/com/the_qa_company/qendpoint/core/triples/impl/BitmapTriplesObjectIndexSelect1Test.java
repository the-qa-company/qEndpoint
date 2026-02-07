package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap375Big;
import com.the_qa_company.qendpoint.core.compact.sequence.DynamicSequence;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64Big;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTSpecification;
import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.disk.LargeLongArray;
import com.the_qa_company.qendpoint.core.util.disk.LongArray;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import org.junit.Test;

import java.lang.reflect.Method;

public class BitmapTriplesObjectIndexSelect1Test {
	@Test
	public void objectIndexBuildDoesNotCallSelect1() throws Exception {
		Select1GuardTriples triples = new Select1GuardTriples();
		DynamicSequence seqZ = new SequenceLog64Big(BitUtil.log2(2), 4, true);
		seqZ.set(0, 1);
		seqZ.set(1, 1);
		seqZ.set(2, 2);
		seqZ.set(3, 2);

		DynamicSequence seqY = new SequenceLog64Big(BitUtil.log2(4), 4, true);
		seqY.set(0, 3);
		seqY.set(1, 1);
		seqY.set(2, 2);
		seqY.set(3, 4);

		Bitmap375Big bitmapZ = Bitmap375Big.memory(seqZ.getNumberOfElements());
		for (long i = 0; i < seqZ.getNumberOfElements(); i++) {
			bitmapZ.set(i, true);
		}

		triples.seqY = seqY;
		triples.seqZ = seqZ;
		triples.bitmapZ = bitmapZ;

		Method method = BitmapTriples.class.getDeclaredMethod("createIndexObjectMemoryEfficient",
				ProgressListener.class);
		method.setAccessible(true);
		method.invoke(triples, ProgressListener.ignore());
	}

	private static final class Select1GuardTriples extends BitmapTriples {
		private Select1GuardTriples() throws Exception {
			super(new HDTSpecification());
		}

		@Override
		public Bitmap375Big createBitmap375(java.nio.file.Path baseDir, String name, long size) {
			return Select1GuardBitmap.memory(size);
		}
	}

	private static final class Select1GuardBitmap extends Bitmap375Big {
		private Select1GuardBitmap(LongArray words, java.nio.file.Path location, boolean useDiskSuperIndex,
				long numbits) {
			super(words, location, useDiskSuperIndex);
			trim(numbits);
		}

		public static Select1GuardBitmap memory(long nbits) {
			LongArray words = new LargeLongArray(IOUtil.createLargeArray(numWords(nbits)));
			return new Select1GuardBitmap(words, null, false, nbits);
		}

		@Override
		public long select1(long n) {
			throw new AssertionError("select1 should not be called during object index build");
		}

	}
}
