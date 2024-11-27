package com.the_qa_company.qendpoint.core.merge;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.iterator.utils.FetcherIterator;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;

import java.util.List;

public class HDTMergeJoinIterator extends FetcherIterator<List<HDTMergeJoinIterator.MergeIteratorData>> {
	public static final class MergeIteratorData {
		private final IteratorTripleID it;
		private final TripleComponentRole role;
		private TripleID last;
		private boolean loaded;

		public MergeIteratorData(IteratorTripleID it, TripleComponentRole role) {
			this.it = it;
			this.role = role;
		}

		public long getSeekLayer(TripleID id) {
			return switch (role) {
			case OBJECT -> id.getObject();
			case PREDICATE -> id.getPredicate();
			case SUBJECT -> id.getSubject();
			case GRAPH -> id.getGraph();
			};
		}

		/**
		 * goto a layer
		 *
		 * @param id layer
		 * @return if we reach the end
		 */
		public boolean gotoLayer(long id) {
			while (hasNext()) {
				if (getSeekLayer(last) >= id) {
					return false; // good layer or after
				}
				next(); // force next
			}
			return true;
		}

		public boolean hasNext() {
			if (loaded) {
				return true;
			}
			if (!it.hasNext()) {
				return false;
			}

			last = it.next();
			loaded = true;
			return true;
		}

		public TripleID peek() {
			if (hasNext()) {
				return last;
			}
			return null;
		}

		public TripleID next() {
			if (hasNext()) {
				loaded = false;
				return last;
			}
			return null;
		}
	}

	private final List<MergeIteratorData> iterators;
	private boolean loaded;

	public HDTMergeJoinIterator(List<MergeIteratorData> iterators) {
		this.iterators = iterators;
	}

	private void moveNext() {
		if (!loaded) {
			loaded = true;
			return; // start
		}

		int minIdx = 0;
		if (!iterators.get(minIdx).hasNext()) {
			return;
		}
		TripleID minVal = iterators.get(minIdx).peek();
		TripleComponentOrder minOrder = iterators.get(minIdx).it.getOrder();

		for (int i = 1; i < iterators.size(); i++) {
			MergeIteratorData d = iterators.get(i);
			if (!d.hasNext()) {
				return;
			}
			TripleID peek = d.peek();

			if (peek == null) {
				return; // end
			}

			TripleComponentOrder ord = d.it.getOrder();
			if (peek.compareTo(minVal, ord, minOrder) < 0) {
				minVal = peek;
				minOrder = ord;
				minIdx = i;
			}
		}

		// move to next using this iterator
		iterators.get(minIdx).next();
	}

	private boolean seekAll() {
		MergeIteratorData it1 = iterators.get(0);
		if (!it1.hasNext()) {
			return false; // no data
		}
		long seek = it1.getSeekLayer(it1.peek());
		for (int i = 1; i < iterators.size(); i++) {
			MergeIteratorData d = iterators.get(i);

			if (d.gotoLayer(seek)) {
				return false; // too far
			}

			long seekNext = d.getSeekLayer(d.peek());

			if (seekNext != seek) {
				seek = seekNext;
				i = -1; // to compensate i++
			}
		}

		return true;
	}

	@Override
	protected List<MergeIteratorData> getNext() {
		moveNext();
		if (!seekAll())
			return null;

		// all the iterators are peeked with the same layer, we can read
		return iterators;
	}

}
