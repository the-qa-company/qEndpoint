/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/
 * iterator/MergedIterator.java $ Revision: $Rev: 191 $ Last modified: $Date:
 * 2013-03-03 11:41:43 +0000 (dom, 03 mar 2013) $ Last modified by: $Author:
 * mario.arias $ This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; version 3.0 of the License. This
 * library is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 * Contacting the authors: Mario Arias: mario.arias@deri.org Javier D.
 * Fernandez: jfergar@infor.uva.es Miguel A. Martinez-Prieto:
 * migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.iterator.utils;

import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;

import java.util.Comparator;
import java.util.Iterator;

/**
 * Given two iterators of sorted elements, returns the merge of the two in
 * order, discarding duplicates. Assumes that the input didn't have any
 * duplicates.
 *
 * @author mario.arias
 */
public class MergedIterator<T> implements Iterator<T> {
	private final Iterator<T> left;
	private final Iterator<T> right;
	private T currentLeft, currentRight;
	private final Comparator<T> comparator;
	private final Annotator<T> annotator;

	public enum Pos {
		LEFT, RIGHT, BOTH
	}

	public interface Annotator<T> {
		void annotate(T el, Pos p);
	}

	public MergedIterator(Iterator<T> left, Iterator<T> right, Comparator<T> comparator) {
		this(left, right, comparator, null);
	}

	public MergedIterator(Iterator<T> left, Iterator<T> right, Comparator<T> comparator, Annotator<T> annotator) {
		this.left = left;
		this.right = right;
		this.comparator = comparator;
		this.annotator = annotator;

		advanceLeft();
		advanceRight();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		return currentLeft != null || currentRight != null;
	}

	private T advanceLeft() {
		T tmp = currentLeft;
		if (left.hasNext()) {
			currentLeft = left.next();
		} else {
			currentLeft = null;
		}
		return tmp;
	}

	private T advanceRight() {
		T tmp = currentRight;
		if (right.hasNext()) {
			currentRight = right.next();
		} else {
			currentRight = null;
		}
		return tmp;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public T next() {
		// No more on right
		if (currentLeft != null && currentRight == null) {
			return advanceLeft();
		}
		// No more on left
		if (currentLeft == null && currentRight != null) {
			return advanceRight();
		}

		// Compare
		int cmp = comparator.compare(currentLeft, currentRight);
		if (cmp == 0) {
			// Equal, return one, advance both to discard duplicate.
			if (annotator != null)
				annotator.annotate(currentLeft, Pos.BOTH);
			advanceRight();
			return advanceLeft();
		} else if (cmp > 0) {
			if (annotator != null)
				annotator.annotate(currentLeft, Pos.LEFT);
			return advanceLeft();
		} else {
			if (annotator != null)
				annotator.annotate(currentLeft, Pos.RIGHT);
			return advanceRight();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		throw new NotImplementedException();
	}

}
