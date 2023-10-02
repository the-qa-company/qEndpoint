/**
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/iterator/DictionaryTranslateIterator.java
 * $ Revision: $Rev: 191 $ Last modified: $Date: 2013-03-03 11:41:43 +0000 (dom,
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

package com.the_qa_company.qendpoint.core.iterator;

import com.the_qa_company.qendpoint.core.dictionary.DictionaryPrivate;
import com.the_qa_company.qendpoint.core.dictionary.impl.OptimizedExtractor;
import com.the_qa_company.qendpoint.core.enums.ResultEstimationType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.quad.QuadString;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.TripleString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Iterator of TripleStrings based on IteratorTripleID
 */
public class DictionaryTranslateIteratorBuffer implements IteratorTripleString {
	private static class TripleIdWithIndex {
		final TripleID triple;
		final TriplePositionSupplier index;

		TripleIdWithIndex(TripleID triple, TriplePositionSupplier index) {
			this.triple = triple;
			this.index = index;
		}
	}

	private static int DEFAULT_BLOCK_SIZE = 10000;
	private TriplePositionSupplier lastPosition;
	final int blockSize;

	SuppliableIteratorTripleID iterator;
	OptimizedExtractor dictionary;
	CharSequence s, p, o, g;

	List<TripleIdWithIndex> triples;
	Iterator<TripleIdWithIndex> child = Collections.emptyIterator();

	Map<Long, CharSequence> mapSubject, mapPredicate, mapObject, mapGraph;

	long lastSid, lastPid, lastOid, lastGid;
	CharSequence lastSstr, lastPstr, lastOstr, lastGstr;

	boolean isHDTQ;

	public DictionaryTranslateIteratorBuffer(SuppliableIteratorTripleID iteratorTripleID, DictionaryPrivate dictionary,
			CharSequence s, CharSequence p, CharSequence o) {
		this(iteratorTripleID, dictionary, s, p, o, null, DEFAULT_BLOCK_SIZE, false);
	}

	public DictionaryTranslateIteratorBuffer(SuppliableIteratorTripleID iteratorTripleID, DictionaryPrivate dictionary,
			CharSequence s, CharSequence p, CharSequence o, int blockSize) {
		this(iteratorTripleID, dictionary, s, p, o, null, blockSize, false);
	}

	public DictionaryTranslateIteratorBuffer(SuppliableIteratorTripleID iteratorTripleID, DictionaryPrivate dictionary,
			CharSequence s, CharSequence p, CharSequence o, CharSequence g) {
		this(iteratorTripleID, dictionary, s, p, o, g, DEFAULT_BLOCK_SIZE, g != null);
	}

	public DictionaryTranslateIteratorBuffer(SuppliableIteratorTripleID iteratorTripleID, DictionaryPrivate dictionary,
			CharSequence s, CharSequence p, CharSequence o, CharSequence g, int blockSize) {
		this(iteratorTripleID, dictionary, s, p, o, g, blockSize, true);
	}

	private DictionaryTranslateIteratorBuffer(SuppliableIteratorTripleID iteratorTripleID, DictionaryPrivate dictionary,
			CharSequence s, CharSequence p, CharSequence o, CharSequence g, int blockSize, boolean isHDTQ) {
		this.blockSize = blockSize;
		this.iterator = iteratorTripleID;
		this.dictionary = dictionary.createOptimizedMapExtractor();

		this.s = s == null ? "" : s;
		this.p = p == null ? "" : p;
		this.o = o == null ? "" : o;
		this.g = g == null ? "" : g;
		this.isHDTQ = isHDTQ;
	}

	private void reset() {
		triples = new ArrayList<>(blockSize);

		if (s.length() == 0) {
			mapSubject = new HashMap<>(blockSize);
		}

		if (p.length() == 0) {
			mapPredicate = new HashMap<>();
		}

		if (o.length() == 0) {
			mapObject = new HashMap<>(blockSize);
		}

		if (g.length() == 0) {
			mapGraph = new HashMap<>(blockSize);
		}
	}

	private void fill(long[] arr, int count, Map<Long, CharSequence> map, TripleComponentRole role) {
		Arrays.sort(arr, 0, count);

		long last = -1;
		for (int i = 0; i < count; i++) {
			long val = arr[i];

			if (val != last) {
				CharSequence str = dictionary.idToString(val, role);

				map.put(val, str);

				last = val;
			}
		}
	}

	private void fetchBlock() {
		reset();

		long[] arrSubjects = new long[blockSize];
		long[] arrPredicates = new long[blockSize];
		long[] arrObjects = new long[blockSize];
		long[] arrGraphs = new long[blockSize];

		int count = 0;
		for (int i = 0; i < blockSize && iterator.hasNext(); i++) {
			TripleID t = new TripleID(iterator.next());
			TriplePositionSupplier index = iterator.getLastTriplePositionSupplier();

			TripleIdWithIndex itid = new TripleIdWithIndex(t, index);

			triples.add(itid);

			if (s.length() == 0)
				arrSubjects[count] = t.getSubject();
			if (p.length() == 0)
				arrPredicates[count] = t.getPredicate();
			if (o.length() == 0)
				arrObjects[count] = t.getObject();
			if (g.length() == 0)
				arrGraphs[count] = t.getGraph();

			count++;
		}
		if (s.length() == 0) {
			fill(arrSubjects, count, mapSubject, TripleComponentRole.SUBJECT);
		}
		if (p.length() == 0) {
			fill(arrPredicates, count, mapPredicate, TripleComponentRole.PREDICATE);
		}
		if (o.length() == 0) {
			fill(arrObjects, count, mapObject, TripleComponentRole.OBJECT);
		}
		if (g.length() == 0 && isHDTQ) {
			fill(arrGraphs, count, mapGraph, TripleComponentRole.GRAPH);
		}

		this.child = triples.iterator();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		boolean more = child.hasNext() || iterator.hasNext();
		if (!more) {
			mapSubject = mapPredicate = mapObject = mapGraph = null;
			triples = null;
		}
		return more;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public TripleString next() {
		if (!child.hasNext()) {
			fetchBlock();
		}

		TripleIdWithIndex itid = child.next();
		TripleID triple = itid.triple;
		lastPosition = itid.index;

		if (s.length() != 0) {
			lastSstr = s;
		} else if (triple.getSubject() != lastSid) {
			lastSid = triple.getSubject();
			lastSstr = mapSubject.get(lastSid);
		}

		if (p.length() != 0) {
			lastPstr = p;
		} else if (triple.getPredicate() != lastPid) {
			lastPid = triple.getPredicate();
			lastPstr = mapPredicate.get(lastPid);
		}

		if (o.length() != 0) {
			lastOstr = o;
		} else if (triple.getObject() != lastOid) {
			lastOid = triple.getObject();
			lastOstr = mapObject.get(lastOid);
		}

		if (g.length() != 0) {
			lastGstr = g;
		} else if (triple.getGraph() != lastGid) {
			lastGid = triple.getGraph();
			lastGstr = mapGraph.get(lastGid);
		}

		if (isHDTQ) {
			return new QuadString(lastSstr, lastPstr, lastOstr, lastGstr);
		}

		return new TripleString(lastSstr, lastPstr, lastOstr);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		iterator.remove();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleString#goToStart()
	 */
	@Override
	public void goToStart() {
		iterator.goToStart();
		this.reset();
	}

	@Override
	public long estimatedNumResults() {
		return iterator.estimatedNumResults();
	}

	@Override
	public ResultEstimationType numResultEstimation() {
		return iterator.numResultEstimation();
	}

	@Override
	public long getLastTriplePosition() {
		return lastPosition.compute();
	}

	public static void setBlockSize(int size) {
		DEFAULT_BLOCK_SIZE = size;
	}

	public static int getBlockSize() {
		return DEFAULT_BLOCK_SIZE;
	}
}
