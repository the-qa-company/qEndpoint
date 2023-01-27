package com.the_qa_company.qendpoint.core.dictionary.impl.kcat;

import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.iterator.utils.CombinedIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.MapFilterIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.MapIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.MergeExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.PeekIterator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Antoine Willerval
 */
public class GroupBySubjectMapIterator implements Iterator<TripleID> {
	private final PeekIterator<TripleID> mergeIterator;
	private final List<TripleID> groupList = new ArrayList<>();
	private Iterator<TripleID> groupListIterator;
	private TripleID next;

	private GroupBySubjectMapIterator(Iterator<TripleID> mergeIterator) {
		this.mergeIterator = new PeekIterator<>(mergeIterator);
	}

	@Override
	public boolean hasNext() {
		if (next != null) {
			return true;
		}

		// get triples from the group
		if (groupListIterator != null) {
			if (groupListIterator.hasNext()) {
				next = groupListIterator.next();
				return true;
			}

			// clear the group and set to new iteration
			groupList.clear();
			groupListIterator = null;
		}

		// do we have more elements?
		if (!mergeIterator.hasNext()) {
			return false;
		}

		TripleID peek = mergeIterator.peek();
		long subject = peek.getSubject();
		long predicate = peek.getPredicate();

		// we add all the elements while the subject/predicate are the same
		do {
			groupList.add(mergeIterator.next().clone());
		} while (mergeIterator.hasNext() && mergeIterator.peek().getSubject() == subject
				&& mergeIterator.peek().getPredicate() == predicate);

		groupList.sort(Comparator.comparingLong(TripleID::getObject));

		groupListIterator = groupList.iterator();

		if (groupListIterator.hasNext()) {
			next = groupListIterator.next();
			return true;
		}

		throw new AssertionError("Empty groupList iterator of non empty list");
	}

	@Override
	public TripleID next() {
		if (!hasNext()) {
			return null;
		}
		try {
			return next;
		} finally {
			next = null;
		}
	}

	private static long firstSubjectTripleId(HDT hdt) {
		if (hdt.getDictionary().getSubjects().getNumberOfElements() == 0) {
			// no subjects
			return -1;
		}
		IteratorTripleID it = hdt.getTriples().search(new TripleID(hdt.getDictionary().getNshared() + 1, 0, 0));
		if (it.hasNext()) {
			// extract result
			it.next();
			return it.getLastTriplePosition();
		} else {
			return -1;
		}
	}

	/**
	 * create a GroupBy (subject,predicate) Sorted (object) iterator from the
	 * hdts by mapping the triples with the merger if a deleteBitmap is present
	 * for the hdt, the unwanted triples won't be read
	 *
	 * @param merger        the merger
	 * @param hdts          the HDTs to iterate
	 * @param deleteBitmaps delete bitmaps to remove from the HDTs' triples (can
	 *                      be null)
	 * @return sorted iterator of tripleID usable into a BitmapTriples
	 *         generation
	 */
	public static Iterator<TripleID> fromHDTs(KCatMerger merger, HDT[] hdts, List<? extends Bitmap> deleteBitmaps) {
		final long shared = merger.getCountShared();
		// sorted shared
		List<ExceptionIterator<TripleID, RuntimeException>> sharedSubjectIterators = IntStream.range(0, hdts.length)
				.mapToObj(hdtIndex -> {
					// extract hdt elements for this index
					HDT hdt = hdts[hdtIndex];
					Bitmap deleteBitmap = deleteBitmaps == null ? null : deleteBitmaps.get(hdtIndex);

					if (hdt.getTriples().getNumberOfElements() == 0) {
						// no triples
						return ExceptionIterator.<TripleID, RuntimeException>empty();
					}
					// get the first subject triple id
					long firstSubjectTripleId = firstSubjectTripleId(hdt);

					ExceptionIterator<TripleID, RuntimeException> subjectIteratorMapped;
					if (firstSubjectTripleId == -1) {
						// no triples
						subjectIteratorMapped = ExceptionIterator.empty();
					} else {
						// create a subject iterator, mapped to the new IDs
						IteratorTripleID subjectIterator = hdt.getTriples().searchAll();
						subjectIterator.goTo(firstSubjectTripleId);

						subjectIteratorMapped = ExceptionIterator.of(new SharedOnlyIterator(createIdMapper(merger,
								hdtIndex, hdt, subjectIterator, firstSubjectTripleId, deleteBitmap), shared));
					}

					if (shared == 0) {
						return subjectIteratorMapped;
					}

					Iterator<TripleID> sharedIterator = new SharedStopIterator(hdt.getTriples().searchAll(),
							hdt.getDictionary().getNshared());
					Iterator<TripleID> sharedIteratorMapped = new SharedOnlyIterator(
							createIdMapper(merger, hdtIndex, hdt, sharedIterator, 0, deleteBitmap), shared);

					return new MergeExceptionIterator<>(subjectIteratorMapped,
							ExceptionIterator.of(sharedIteratorMapped), TripleID::compareTo);
				}).collect(Collectors.toList());
		// sorted subjects
		List<ExceptionIterator<TripleID, RuntimeException>> subjectIterators = IntStream.range(0, hdts.length)
				.mapToObj(hdtIndex -> {
					// extract hdt elements for this index
					HDT hdt = hdts[hdtIndex];
					Bitmap deleteBitmap = deleteBitmaps == null ? null : deleteBitmaps.get(hdtIndex);

					if (hdt.getTriples().getNumberOfElements() == 0) {
						// no triples
						return ExceptionIterator.<TripleID, RuntimeException>empty();
					}
					// get the first subject triple id
					long firstSubjectTripleId = firstSubjectTripleId(hdt);

					ExceptionIterator<TripleID, RuntimeException> subjectIteratorMapped;
					if (firstSubjectTripleId == -1) {
						// no triples
						subjectIteratorMapped = ExceptionIterator.empty();
					} else {
						// create a subject iterator, mapped to the new IDs
						IteratorTripleID subjectIterator = hdt.getTriples().searchAll();
						subjectIterator.goTo(firstSubjectTripleId);

						subjectIteratorMapped = ExceptionIterator.of(new NoSharedIterator(createIdMapper(merger,
								hdtIndex, hdt, subjectIterator, firstSubjectTripleId, deleteBitmap), shared));
					}

					if (shared == 0 || deleteBitmap == null) {
						// we don't recompute the shared if no delete bitmap is
						// here because it wouldn't make sense that
						// a shared element lost its shared status without
						// deleting statements
						return subjectIteratorMapped;
					}

					long sharedCount = hdt.getDictionary().getNshared();
					Iterator<TripleID> sharedIterator = new SharedStopIterator(hdt.getTriples().searchAll(),
							sharedCount);
					Iterator<TripleID> sharedIteratorMapped = new NoSharedIterator(
							createIdMapper(merger, hdtIndex, hdt, sharedIterator, 0, deleteBitmap), shared);

					return new MergeExceptionIterator<>(subjectIteratorMapped,
							ExceptionIterator.of(sharedIteratorMapped), TripleID::compareTo);
				}).collect(Collectors.toList());

		return new GroupBySubjectMapIterator(new NoDupeTripleIDIterator(CombinedIterator.combine(List.of(
				MergeExceptionIterator.buildOfTree(Function.identity(), GroupBySubjectMapIterator::compareSP,
						sharedSubjectIterators, 0, sharedSubjectIterators.size()).asIterator(),
				MergeExceptionIterator.buildOfTree(Function.identity(), GroupBySubjectMapIterator::compareSP,
						subjectIterators, 0, subjectIterators.size()).asIterator()))));
	}

	private static Iterator<TripleID> createIdMapper(KCatMerger merger, int hdtIndex, HDT hdt, Iterator<TripleID> it,
			long start, Bitmap deleteBitmap) {
		if (deleteBitmap == null) {
			return new MapIterator<>(it, (tid) -> {
				assert inHDT(tid, hdt);
				return merger.extractMapped(hdtIndex, tid);
			});
		} else {
			return MapFilterIterator.of(it, (tid, index) -> {
				if (deleteBitmap.access(index + start)) {
					return null;
				}
				assert inHDT(tid, hdt);
				return merger.extractMapped(hdtIndex, tid);
			});
		}
	}

	private static int compareSP(TripleID t1, TripleID t2) {
		int compare = Long.compare(t1.getSubject(), t2.getSubject());
		if (compare != 0) {
			return compare;
		}
		return Long.compare(t1.getPredicate(), t2.getPredicate());
	}

	private static boolean inHDT(TripleID id, HDT hdt) {
		long s = id.getSubject();
		long p = id.getPredicate();
		long o = id.getObject();
		return s >= 1 && s <= hdt.getDictionary().getNsubjects() && p >= 1 && p <= hdt.getDictionary().getNpredicates()
				&& o >= 1 && o <= hdt.getDictionary().getNobjects();
	}

	private static class NoDupeTripleIDIterator implements Iterator<TripleID> {
		private TripleID next;
		private final PeekIterator<TripleID> it;

		public NoDupeTripleIDIterator(Iterator<TripleID> it) {
			this.it = new PeekIterator<>(it);
		}

		@Override
		public boolean hasNext() {
			if (next != null) {
				return true;
			}
			if (!it.hasNext()) {
				return false;
			}

			next = it.next();

			assert next.isValid() : "Can't have empty tripleID";

			// pass all the duplicated fields
			while (it.hasNext() && it.peek().equals(next)) {
				it.next();
			}

			return true;
		}

		@Override
		public TripleID next() {
			if (!hasNext()) {
				return null;
			}
			try {
				return next;
			} finally {
				next = null;
			}
		}
	}

	private static class SharedStopIterator implements Iterator<TripleID> {
		private final Iterator<TripleID> it;
		private final long shared;
		private TripleID next;

		private SharedStopIterator(Iterator<TripleID> it, long shared) {
			this.it = it;
			this.shared = shared;
		}

		@Override
		public boolean hasNext() {
			if (next != null) {
				return next.getSubject() <= shared;
			}

			if (!it.hasNext()) {
				return false;
			}

			next = it.next();

			return next.getSubject() <= shared;
		}

		@Override
		public TripleID next() {
			if (!hasNext()) {
				return null;
			}
			try {
				return next;
			} finally {
				next = null;
			}
		}
	}

	private static class SharedOnlyIterator implements Iterator<TripleID> {
		private final Iterator<TripleID> it;
		private final long shared;
		private TripleID next;

		private SharedOnlyIterator(Iterator<TripleID> it, long shared) {
			this.it = it;
			this.shared = shared;
		}

		@Override
		public boolean hasNext() {
			if (next != null) {
				return true;
			}

			// search over the next results
			while (it.hasNext()) {
				TripleID next = it.next();

				// is this element a shared element?
				if (next.getSubject() <= shared) {
					this.next = next;
					return true;
				}
			}

			return false;
		}

		@Override
		public TripleID next() {
			if (!hasNext()) {
				return null;
			}
			try {
				return next;
			} finally {
				next = null;
			}
		}
	}

	private static class NoSharedIterator implements Iterator<TripleID> {
		private final Iterator<TripleID> it;
		private final long shared;
		private TripleID next;

		private NoSharedIterator(Iterator<TripleID> it, long shared) {
			this.it = it;
			this.shared = shared;
		}

		@Override
		public boolean hasNext() {
			if (next != null) {
				return true;
			}

			// search over the next results
			while (it.hasNext()) {
				TripleID next = it.next();

				// is this element a shared element?
				if (next.getSubject() > shared) {
					this.next = next;
					return true;
				}
			}

			return false;
		}

		@Override
		public TripleID next() {
			if (!hasNext()) {
				return null;
			}
			try {
				return next;
			} finally {
				next = null;
			}
		}
	}
}
