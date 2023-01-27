/**
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-jena/src/org/rdfhdt/hdtjena/cache/DictionaryCacheLRI.java
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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Least-Recently-Inserted Cache. Removes the oldest entries inserted. Can
 * result in cache misses but it is very fast. It uses an array-based circular
 * buffer to remove old entries
 *
 * @author mario.arias
 */
public class DictionaryCacheLRI<T> implements DictionaryCache<T> {

	private final Map<Long, T> cache;
	private final long[] arr;
	private int ptr = 0;
	private final int size;

	public DictionaryCacheLRI(int size) {
		this.size = size;
		arr = new long[size];
		cache = new ConcurrentHashMap<>(size);
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.jena.DictionaryNodeCache#getNode(int)
	 */
	@Override
	public T get(long id) {
		return cache.get(id);
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.jena.DictionaryNodeCache#setNode(long, T)
	 */
	@Override
	public void put(long id, T node) {
		cache.put(id, node);
		if (cache.size() > size) {
			cache.remove(arr[ptr]);
		}
		synchronized (this) {
			arr[ptr] = id;
			ptr = (ptr + 1) % size;
		}
	}

	@Override
	public int size() {
		return cache.size();
	}

	@Override
	public void clear() {
		cache.clear();
		// No need to clear the circular buffer.
	}

}
