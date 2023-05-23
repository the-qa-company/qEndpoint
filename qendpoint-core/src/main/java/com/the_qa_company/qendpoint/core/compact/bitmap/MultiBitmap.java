package com.the_qa_company.qendpoint.core.compact.bitmap;

import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64Big;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64BigDisk;

import java.io.Closeable;
import java.nio.file.Path;

/**
 * Object to store multiple bitmaps with the bits being close to each other
 * (better for disk reading), can't store more than 64 bitmaps in a same object
 * due to
 */
public interface MultiBitmap extends Closeable {
	static MultiBitmap disk(Path location, int count, long size) {
		if (count <= 0) {
			throw new IllegalArgumentException("Can't have a negative or nul count");
		}
		if (count > 64) {
			throw new IllegalArgumentException("Can't store more than 64 bitmaps in a MultiBitmap");
		}
		return new MultiBitmapImpl(new SequenceLog64BigDisk(location, count, size), count);
	}

	static MultiBitmap memory(int count, long size) {
		return new MultiBitmapImpl(new SequenceLog64Big(count, size), count);
	}

	/**
	 * get a sub bitmap
	 *
	 * @param index index
	 * @return bitmap
	 */
	ModifiableBitmap getBitmap(int index);
}
