/**
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-jena/src/org/rdfhdt/hdtjena/cache/DictionaryCacheArray.java
 * $ Revision: $Rev: 190 $ Last modified: $Date: 2013-03-03 11:30:03 +0000 (dom,
 * 03 mar 2013) $ Last modified by: $Author: mario.arias $ This library is free
 * software; you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation;
 * version 3.0 of the License. This library is distributed in the hope that it
 * will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 * of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
 * General Public License for more details. You should have received a copy of
 * the GNU Lesser General Public License along with this library; if not, write
 * to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston,
 * MA 02110-1301 USA Contacting the authors: Mario Arias: mario.arias@deri.org
 * Javier D. Fernandez: jfergar@infor.uva.es Miguel A. Martinez-Prieto:
 * migumar2@infor.uva.es
 */

package com.the_qa_company.qendpoint.core.cache;

/**
 * Cache using an array. Assumes valid id&gt;0
 *
 * @author mario.arias
 */
public class DictionaryCacheArray<T> implements DictionaryCache<T> {

	private Object[] array;
	final int capacity;
	int numentries;

	public DictionaryCacheArray(int capacity) {
		array = null;
		this.capacity = capacity;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.jena.DictionaryNodeCache#getNode(long)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public T get(long id) {
		if (array == null) {
			return null;
		}
		if (id > array.length) {
			return null; // Not found
		}
		return (T) array[(int) (id - 1)];
	}

	@Override
	public void put(long id, T node) {
		if (array == null) {
			array = new Object[(int) capacity];
		}
		if (array.length < id) {
//			System.err.println("Warning: Trying to insert a value in cache out of bounds: "+id + " / "+array.length);
			return;
		}
		if (array[(int) (id - 1)] == null) {
			numentries++;
		}
		array[(int) (id - 1)] = node;
	}

	@Override
	public int size() {
		return numentries;
	}

	@Override
	public void clear() {
		array = null;
	}

}
