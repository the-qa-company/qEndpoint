/**
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-jena/src/org/rdfhdt/hdtjena/cache/DictionaryCacheArrayWeak.java
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

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;

/**
 * @author mario.arias
 */
public class DictionaryCacheArrayWeak<T> implements DictionaryCache<T> {

	private final Reference<T>[] array;

	@SuppressWarnings("unchecked")
	public DictionaryCacheArrayWeak(int capacity) {
		array = new Reference[capacity];
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.jena.DictionaryNodeCache#getNode(long)
	 */
	@Override
	public T get(long id) {
		Reference<T> ref = array[(int) (id - 1)];
		if (ref != null) {
			return ref.get();
		}

		return null;
	}

	@Override
	public void put(long id, T node) {
		array[(int) (id - 1)] = new WeakReference<>(node);
	}

	@Override
	public int size() {
		// Can't estimate. We don't know how many the GC disposed.
		return 0;
	}

	@Override
	public void clear() {
		// FIXME: Implement concurrency friendly.
	}

}
