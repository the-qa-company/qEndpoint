package com.the_qa_company.qendpoint.core.dictionary.impl.utilDiff;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilCat.CatElement;
import com.the_qa_company.qendpoint.core.util.string.ByteString;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Iterator keeping only the element marked with a true in a bitmap
 */
public class DiffWrapper implements Iterator<CatElement> {

	public final Iterator<? extends CharSequence> sectionIter;
	public final Bitmap bitmap;
	public final ByteString iterName;

	/**
	 * create a diffWrapper of the iterator sectionIter with the bitmap bitmap
	 *
	 * @param sectionIter the iterator to wrap
	 * @param bitmap      the bitmap to tell which element to keep
	 * @param iterName    the name of the section of the iterator
	 */
	public DiffWrapper(Iterator<? extends CharSequence> sectionIter, Bitmap bitmap, ByteString iterName) {
		this.sectionIter = sectionIter;
		this.bitmap = bitmap;
		this.iterName = iterName;
	}

	private CatElement next;
	private long count = 0;

	@Override
	public boolean hasNext() {
		while (sectionIter.hasNext()) {
			ByteString element = ByteString.of(sectionIter.next());
			if (bitmap.access(count)) {
				// we need to keep this element
				List<CatElement.IteratorPlusPosition> IDs = new ArrayList<>();

				IDs.add(new CatElement.IteratorPlusPosition(iterName, count + 1));
				next = new CatElement(element, IDs);
				count++;
				return true;
			}
			count++;
		}
		return false;
	}

	@Override
	public CatElement next() {
		return next;
	}
}
