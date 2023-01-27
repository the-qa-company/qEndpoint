/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/compact
 * /sequence/DynamicSequence.java $ Revision: $Rev: 191 $ Last modified: $Date:
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

package com.the_qa_company.qendpoint.core.compact.sequence;

import com.the_qa_company.qendpoint.core.util.disk.LongArray;

/**
 * @author mario.arias
 */
public interface DynamicSequence extends Sequence, LongArray {

	/**
	 * Append a new value after the last position, increasing the number of
	 * elements by one.
	 *
	 * @param value the value to append
	 */
	void append(long value);

	/**
	 * Trim the internal data structure to fit the actual number of elements.
	 */
	void trimToSize();

	/**
	 * Trim the internal data structure to fit the actual number of elements.
	 * Use advanced algorithm to reduce the size to the minimum, even if it is
	 * costly.
	 */
	void aggressiveTrimToSize();

	@Override
	default long length() {
		return getNumberOfElements();
	}

	@Override
	void resize(long size);
}
