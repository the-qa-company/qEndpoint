package com.the_qa_company.qendpoint.core.iterator.utils;

import com.the_qa_company.qendpoint.core.triples.IndexedNode;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * Merge iterator to merge {@link IndexedNode} with delta to reduce the compare
 * count
 *
 * @param <E> fetcher exception type
 * @author Antoine Willerval
 */
public abstract class IndexNodeDeltaMergeExceptionIterator<E extends Exception>
		implements ExceptionIterator<IndexedNode, E> {
	/**
	 * Create a tree of merge iterators from an array of element
	 *
	 * @param itFunction a function to create an iterator from an element
	 * @param array      the elements
	 * @param length     the number of elements
	 * @param <I>        input of the element
	 * @param <E>        exception returned by the iterator
	 * @return the iterator
	 */
	public static <I, E extends Exception> ExceptionIterator<IndexedNode, E> buildOfTree(
			Function<I, IndexNodeDeltaFetcher<E>> itFunction, I[] array, int length) {
		return buildOfTree(itFunction, array, 0, length);
	}

	/**
	 * Create a tree of merge iterators from an array of element
	 *
	 * @param itFunction a function to create an iterator from an element
	 * @param array      the elements
	 * @param start      the start of the array (inclusive)
	 * @param end        the end of the array (exclusive)
	 * @param <E>        exception returned by the iterator
	 * @return the iterator
	 */
	public static <I, E extends Exception> ExceptionIterator<IndexedNode, E> buildOfTree(
			Function<I, IndexNodeDeltaFetcher<E>> itFunction, I[] array, int start, int end) {
		return buildOfTree(itFunction, Arrays.asList(array), start, end);
	}

	/**
	 * Create a tree of merge iterators from an array of element
	 *
	 * @param itFunction a function to create an iterator from an element
	 * @param array      the elements
	 * @param start      the start of the array (inclusive)
	 * @param end        the end of the array (exclusive)
	 * @param <E>        exception returned by the iterator
	 * @return the iterator
	 */
	public static <I, E extends Exception> ExceptionIterator<IndexedNode, E> buildOfTree(
			Function<I, IndexNodeDeltaFetcher<E>> itFunction, List<I> array, int start, int end) {
		return buildOfTree0(itFunction, array, start, end);
	}

	/**
	 * Create a tree of merge iterators from an array of element
	 *
	 * @param array  the elements
	 * @param length the number of elements
	 * @param <I>    input of the element
	 * @param <E>    exception returned by the iterator
	 * @return the iterator
	 */
	public static <I extends IndexNodeDeltaFetcher<E>, E extends Exception> ExceptionIterator<IndexedNode, E> buildOfTree(
			I[] array, int length) {
		return buildOfTree(i -> i, array, 0, length);
	}

	/**
	 * Create a tree of merge iterators from an array of element
	 *
	 * @param array the elements
	 * @param <I>   input of the element
	 * @param <E>   exception returned by the iterator
	 * @return the iterator
	 */
	public static <I extends IndexNodeDeltaFetcher<E>, E extends Exception> ExceptionIterator<IndexedNode, E> buildOfTree(
			I[] array) {
		return buildOfTree(i -> i, array, 0, array.length);
	}

	/**
	 * Create a tree of merge iterators from an array of element
	 *
	 * @param array the elements
	 * @param start the start of the array (inclusive)
	 * @param end   the end of the array (exclusive)
	 * @param <E>   exception returned by the iterator
	 * @return the iterator
	 */
	public static <I extends IndexNodeDeltaFetcher<E>, E extends Exception> ExceptionIterator<IndexedNode, E> buildOfTree(
			I[] array, int start, int end) {
		return buildOfTree(i -> i, Arrays.asList(array), start, end);
	}

	/**
	 * Create a tree of merge iterators from an array of element
	 *
	 * @param array the elements
	 * @param start the start of the array (inclusive)
	 * @param end   the end of the array (exclusive)
	 * @param <E>   exception returned by the iterator
	 * @return the iterator
	 */
	public static <I extends IndexNodeDeltaFetcher<E>, E extends Exception> ExceptionIterator<IndexedNode, E> buildOfTree(
			List<I> array, int start, int end) {
		return buildOfTree(i -> i, array, start, end);
	}

	private static <I, E extends Exception> IndexNodeDeltaMergeExceptionIterator<E> buildOfTree0(
			Function<I, IndexNodeDeltaFetcher<E>> itFunction, List<I> array, int start, int end) {
		int length = end - start;
		if (length <= 0) {
			return new ExceptionIteratorEmpty<>();
		}
		if (length == 1) {
			return new ExceptionIteratorMap<>(itFunction.apply(array.get(start)));
		}
		int mid = (start + end) / 2;
		return new ExceptionIteratorMerger<>(buildOfTree0(itFunction, array, start, mid),
				buildOfTree0(itFunction, array, mid, end));
	}

	/**
	 * Compare 2 nodes from a starting delta
	 *
	 * @param delta the start delta
	 * @param node1 the 1st node
	 * @param node2 the 2nd node
	 * @return delta+1 if node1 <= node2 or -delta-1 otherwise
	 */
	static int compareToDelta(int delta, IndexedNode node1, IndexedNode node2) {
		CharSequence cs1 = node1.getNode();
		CharSequence cs2 = node2.getNode();

		int len = Math.min(cs1.length(), cs2.length());

		for (int i = delta; i < len; i++) {
			char a = cs1.charAt(i);
			char b = cs2.charAt(i);
			if (a != b) {
				return a > b ? -(i + 1) : (i + 1);
			}
		}

		return cs1.length() != len ? -(len + 1) : (len + 1);
	}

	protected IndexedNode next;
	protected int delta;
	protected int pivot;

	private IndexNodeDeltaMergeExceptionIterator() {
	}

	@Override
	public boolean hasNext() throws E {
		if (next != null) {
			return true;
		}

		fetchNext();

		return peekNext() != null;
	}

	@Override
	public IndexedNode next() throws E {
		if (!hasNext()) {
			return null;
		}
		try {
			return next;
		} finally {
			next = null;
		}
	}

	/**
	 * @return get without switching to the next element of the iterator, should
	 *         be called after a {@link #fetchNext()}
	 */
	public IndexedNode peekNext() {
		return next;
	}

	/**
	 * @return the delta of the last next element, can be after {@link #next()}
	 *         or after {@link #fetchNext()}
	 */
	public int getDelta() {
		return delta;
	}

	/**
	 * fetch the next element, will update the {@link #peekNext()} return value
	 * and the {@link #getDelta()} return value
	 *
	 * @throws E fetch exception
	 */
	public abstract void fetchNext() throws E;

	public void printMergeTree() {
		printMergeTree(0);
	}

	protected abstract void printMergeTree(int depth);

	/**
	 * Implementation of a fetcher to get a node with its delta
	 *
	 * @param <E> fetch exception
	 */
	public interface IndexNodeDeltaFetcher<E extends Exception> {
		/**
		 * @return the next node
		 * @throws E fetch exception
		 */
		IndexedNode fetchNode() throws E;

		/**
		 * @return the delta of the last next node with the previous last-last
		 *         next node
		 * @throws E fetch exception
		 */
		int lastDelta() throws E;
	}

	static class ExceptionIteratorMap<E extends Exception> extends IndexNodeDeltaMergeExceptionIterator<E> {
		private final IndexNodeDeltaFetcher<E> iterator;

		public ExceptionIteratorMap(IndexNodeDeltaFetcher<E> iterator) {
			this.iterator = iterator;
		}

		@Override
		public void fetchNext() throws E {
			next = iterator.fetchNode();
			delta = iterator.lastDelta();
		}

		@Override
		protected void printMergeTree(int depth) {
			System.out.println("  ".repeat(depth) + "Leaf[" + iterator + "]");
		}
	}

	static class ExceptionIteratorEmpty<E extends Exception> extends IndexNodeDeltaMergeExceptionIterator<E> {
		@Override
		public void fetchNext() {
		}

		@Override
		protected void printMergeTree(int depth) {
			System.out.println("  ".repeat(depth) + "Empty");
		}
	}

	static class ExceptionIteratorMerger<E extends Exception> extends IndexNodeDeltaMergeExceptionIterator<E> {
		private final IndexNodeDeltaMergeExceptionIterator<E> it1;
		private final IndexNodeDeltaMergeExceptionIterator<E> it2;
		private IndexedNode last1;
		private IndexedNode last2;
		private boolean send1;

		public ExceptionIteratorMerger(IndexNodeDeltaMergeExceptionIterator<E> it1,
				IndexNodeDeltaMergeExceptionIterator<E> it2) {
			this.it1 = it1;
			this.it2 = it2;
		}

		@Override
		public void fetchNext() throws E {
			if (last1 == null) {
				it1.fetchNext();
				last1 = it1.peekNext();
			}
			if (last2 == null) {
				it2.fetchNext();
				last2 = it2.peekNext();
			}

			// stop fetcher
			if (last1 == null || last2 == null) {
				if (last1 != null) {
					// send last1 if no last1
					next = last1;
					last1 = null;
				} else if (last2 != null) {
					// send last2 if no last1
					next = last2;
					last2 = null;
				} else {
					next = null;
				}
				// else: stop iteration
				return;
			}

			// 2 nodes to compare

			int delta1 = it1.getDelta();
			int delta2 = it2.getDelta();

			// minimum start compare
			int minDelta = Math.min(delta1, delta2);

			if (pivot < minDelta) {
				// no need to check the values, delta is higher than the diff,
				// resend the same value
				if (send1) {
					next = last1;
					last1 = null;
				} else {
					next = last2;
					last2 = null;
				}

				return;
			}

			// we need to compare from at least minDelta chars
			int deltaCompare = compareToDelta(minDelta, last1, last2);

			if (deltaCompare < 0) {
				// node1 > node2 -> send node2
				next = last2;
				pivot = -deltaCompare - 1;
				if (!send1) {
					// the last send was the send1, we can send the real delta
					delta = delta2;
				} else {
					// not the same, we need to compare to get the new delta
					delta = 0;
				}
				last2 = null;
				send1 = false;
			} else {
				// node1 < node2 -> send node1
				next = last1;
				pivot = deltaCompare - 1;
				if (send1) {
					// the last send was the send2, we can send the real delta
					delta = delta1;
				} else {
					// not the same, we need to compare to get the new delta
					delta = 0;
				}
				last1 = null;
				send1 = true;
			}
		}

		@Override
		protected void printMergeTree(int depth) {
			System.out.println("  ".repeat(depth) + "Merge");
			it1.printMergeTree(depth + 1);
			it2.printMergeTree(depth + 1);
		}
	}
}
