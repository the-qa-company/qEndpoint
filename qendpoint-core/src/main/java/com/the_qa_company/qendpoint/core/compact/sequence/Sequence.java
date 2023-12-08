/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/compact
 * /sequence/Sequence.java $ Revision: $Rev: 191 $ Last modified: $Date:
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

import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.disk.ReadOnlyLongArray;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

/**
 * The array interface represents compact sequences of integers.
 */
public interface Sequence extends ReadOnlyLongArray, Closeable {

	/**
	 * Adds an element to the array
	 *
	 * @param elements The element to be added to the array
	 */
	void add(Iterator<Long> elements);

	/**
	 * Gets the element in a specific position
	 *
	 * @param position The position of the element to be returned
	 * @return int
	 */
	long get(long position);

	/**
	 * Gets the total number of elements in the array
	 *
	 * @return int
	 */
	long getNumberOfElements();

	@Override
	default long length() {
		return getNumberOfElements();
	}

	/**
	 * Return the size of the data structure in bytes
	 */
	long size();

	/**
	 * @return size of the components (in bits)
	 */
	int sizeOf();

	/**
	 * Saves the array to an OutputStream
	 *
	 * @param output The OutputStream to be saved to
	 * @throws IOException
	 */
	void save(OutputStream output, ProgressListener listener) throws IOException;

	/**
	 * Loads a array from an InputStream
	 *
	 * @param input The InputStream to load from
	 */
	void load(InputStream input, ProgressListener listener) throws IOException;

	/**
	 * Type identifier of this Stream.
	 */
	String getType();
}
