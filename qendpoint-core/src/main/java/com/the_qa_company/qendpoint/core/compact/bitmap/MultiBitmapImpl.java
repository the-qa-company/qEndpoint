package com.the_qa_company.qendpoint.core.compact.bitmap;

import com.the_qa_company.qendpoint.core.compact.sequence.DynamicSequence;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;

import java.io.IOException;

public class MultiBitmapImpl implements MultiBitmap {
	private final DynamicSequence sequence;
	private final ModifiableBitmap[] sub;

	MultiBitmapImpl(DynamicSequence sequence, int log) {
		this.sequence = sequence;
		sub = new ModifiableBitmap[log];

		for (int i = 0; i < log; i++) {
			sub[i] = new SubBitmap(i);
		}
	}

	@Override
	public ModifiableBitmap getBitmap(int index) {
		return sub[index];
	}

	@Override
	public void close() throws IOException {
		sequence.close();
	}

	private class SubBitmap implements SimpleModifiableBitmap {
		private final int index;

		private SubBitmap(int index) {
			this.index = index;
		}

		@Override
		public boolean access(long position) {
			return ((sequence.get(position) >>> index) & 1) == 1;
		}

		@Override
		public long getNumBits() {
			return sequence.getNumberOfElements();
		}

		@Override
		public long getSizeBytes() {
			return sequence.getNumberOfElements() * sequence.sizeOf() / sub.length;
		}

		@Override
		public String getType() {
			return HDTVocabulary.BITMAP_TYPE_PLAIN;
		}

		@Override
		public void set(long position, boolean value) {
			long old = sequence.get(position);
			if ((((old >>> index) & 1) == 1) != value) {
				if (value) {
					sequence.set(position, old | (1L << index));
				} else {
					sequence.set(position, old & ~(1L << index));
				}
			}
		}

		@Override
		public void append(boolean value) {
			throw new NotImplementedException("Can't append in a MultiBitmap");
		}
	}
}
