package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

public class MergeExceptionIterator<T, E extends Exception> implements ExceptionIterator<T, E> {
	public static final String MERGE_STRATEGY_SYSTEM_PROPERTY = "qendpoint.merge.exception.iterator.strategy";

	public enum MergeStrategy {
		MERGE_ITERATOR, LOSER_TREE
	}

	private static final MergeStrategy DEFAULT_MERGE_STRATEGY = MergeStrategy.LOSER_TREE;
	private static volatile MergeStrategy mergeStrategyOverride;

	/**
	 * Create a tree of merge iterators from an array of element
	 *
	 * @param itFunction a function to create an iterator from an element
	 * @param comp       comparator for the merge iterator
	 * @param array      the elements
	 * @param length     the number of elements
	 * @param <I>        input of the element
	 * @param <T>        type of the element in the iterator
	 * @param <E>        exception returned by the iterator
	 * @return the iterator
	 */
	public static <I, T, E extends Exception> ExceptionIterator<T, E> buildOfTree(
			Function<I, ExceptionIterator<T, E>> itFunction, Comparator<T> comp, I[] array, int length) {
		return buildOfTree(itFunction, comp, array, 0, length);
	}

	/**
	 * Create a tree of merge iterators from an array of element
	 *
	 * @param itFunction a function to create an iterator from an element
	 * @param comp       comparator for the merge iterator
	 * @param array      the elements
	 * @param start      the start of the array (inclusive)
	 * @param end        the end of the array (exclusive)
	 * @param <T>        type of the element
	 * @param <E>        exception returned by the iterator
	 * @return the iterator
	 */
	public static <I, T, E extends Exception> ExceptionIterator<T, E> buildOfTree(
			Function<I, ExceptionIterator<T, E>> itFunction, Comparator<T> comp, I[] array, int start, int end) {
		return buildOfTree(itFunction, comp, Arrays.asList(array), start, end);
	}

	/**
	 * Create a tree of merge iterators from an array of element
	 *
	 * @param itFunction a function to create an iterator from an element
	 * @param comp       comparator for the merge iterator
	 * @param array      the elements
	 * @param start      the start of the array (inclusive)
	 * @param end        the end of the array (exclusive)
	 * @param <T>        type of the element
	 * @param <E>        exception returned by the iterator
	 * @return the iterator
	 */
	public static <I, T, E extends Exception> ExceptionIterator<T, E> buildOfTree(
			Function<I, ExceptionIterator<T, E>> itFunction, Comparator<T> comp, List<I> array, int start, int end) {
		return buildOfTree((index, o) -> itFunction.apply(o), comp, array, start, end);
	}

	/**
	 * Create a tree of merge iterators from an array of element
	 *
	 * @param itFunction a function to create an iterator from an element
	 * @param array      the elements
	 * @param start      the start of the array (inclusive)
	 * @param end        the end of the array (exclusive)
	 * @param <T>        type of the element
	 * @param <E>        exception returned by the iterator
	 * @return the iterator
	 */
	public static <I, T extends Comparable<T>, E extends Exception> ExceptionIterator<T, E> buildOfTree(
			Function<I, ExceptionIterator<T, E>> itFunction, List<I> array, int start, int end) {
		return buildOfTree((index, o) -> itFunction.apply(o), Comparable::compareTo, array, start, end);
	}

	/**
	 * Create a tree of merge iterators from an array of element
	 *
	 * @param array the elements
	 * @param start the start of the array (inclusive)
	 * @param end   the end of the array (exclusive)
	 * @param <T>   type of the element
	 * @param <E>   exception returned by the iterator
	 * @return the iterator
	 */
	public static <T extends Comparable<T>, E extends Exception> ExceptionIterator<T, E> buildOfTree(
			List<ExceptionIterator<T, E>> array, int start, int end) {
		return buildOfTree(Function.identity(), Comparable::compareTo, array, start, end);
	}

	/**
	 * Create a tree of merge iterators from an array of element
	 *
	 * @param array the elements
	 * @param <T>   type of the element
	 * @param <E>   exception returned by the iterator
	 * @return the iterator
	 */
	public static <T extends Comparable<? super T>, E extends Exception> ExceptionIterator<T, E> buildOfTree(
			List<ExceptionIterator<T, E>> array) {
		return buildOfTree(Function.identity(), Comparable::compareTo, array, 0, array.size());
	}

	/**
	 * Create a tree of merge iterators from an array of element
	 *
	 * @param array      the elements
	 * @param comparator comparator for the merge iterator
	 * @param <T>        type of the element
	 * @param <E>        exception returned by the iterator
	 * @return the iterator
	 */
	public static <T, E extends Exception> ExceptionIterator<T, E> buildOfTree(List<ExceptionIterator<T, E>> array,
			Comparator<T> comparator) {
		return buildOfTree(Function.identity(), comparator, array, 0, array.size());
	}

	/**
	 * Create a tree of merge iterators from an array of element
	 *
	 * @param itFunction a function to create an iterator from an element
	 * @param comp       comparator for the merge iterator
	 * @param array      the elements
	 * @param start      the start of the array (inclusive)
	 * @param end        the end of the array (exclusive)
	 * @param <T>        type of the element
	 * @param <E>        exception returned by the iterator
	 * @return the iterator
	 */
	public static <I, T, E extends Exception> ExceptionIterator<T, E> buildOfTree(
			BiFunction<Integer, I, ExceptionIterator<T, E>> itFunction, Comparator<T> comp, List<I> array, int start,
			int end) {
		int length = end - start;
		if (length <= 0) {
			return ExceptionIterator.empty();
		}
		if (length == 1) {
			return itFunction.apply(start, array.get(start));
		}
		if (resolveMergeStrategy() == MergeStrategy.MERGE_ITERATOR) {
			return buildWithMergeIterator(itFunction, comp, array, start, end);
		}
		ArrayList<ExceptionIterator<T, E>> iterators = new ArrayList<>(length);
		for (int i = start; i < end; i++) {
			iterators.add(itFunction.apply(i, array.get(i)));
		}
		return LoserTreeMergeExceptionIterator.merge(iterators, comp);
	}

	private static <I, T, E extends Exception> ExceptionIterator<T, E> buildWithMergeIterator(
			BiFunction<Integer, I, ExceptionIterator<T, E>> itFunction, Comparator<T> comp, List<I> array, int start,
			int end) {
		int length = end - start;
		if (length <= 0) {
			return ExceptionIterator.empty();
		}
		if (length == 1) {
			return itFunction.apply(start, array.get(start));
		}
		int mid = (start + end) / 2;
		return new MergeExceptionIterator<>(buildWithMergeIterator(itFunction, comp, array, start, mid),
				buildWithMergeIterator(itFunction, comp, array, mid, end), comp);
	}

	public static void setMergeStrategyOverride(MergeStrategy strategy) {
		mergeStrategyOverride = Objects.requireNonNull(strategy, "strategy");
	}

	public static void clearMergeStrategyOverride() {
		mergeStrategyOverride = null;
	}

	public static MergeStrategy resolveMergeStrategy() {
		MergeStrategy override = mergeStrategyOverride;
		if (override != null) {
			return override;
		}
		String configured = System.getProperty(MERGE_STRATEGY_SYSTEM_PROPERTY);
		return parseMergeStrategy(configured);
	}

	private static MergeStrategy parseMergeStrategy(String configured) {
		if (configured == null || configured.isBlank()) {
			return DEFAULT_MERGE_STRATEGY;
		}
		String normalized = configured.trim().toLowerCase(Locale.ROOT);
		return switch (normalized) {
		case "merge", "binary", "tree", "mergeiterator", "merge_iterator", "binary_tree" ->
			MergeStrategy.MERGE_ITERATOR;
		case "loser", "losertree", "loser_tree", "priority", "priorityqueue", "priority_queue" ->
			MergeStrategy.LOSER_TREE;
		default -> DEFAULT_MERGE_STRATEGY;
		};
	}

	private final ExceptionIterator<T, E> in1, in2;
	private final Comparator<T> comp;
	private T next;
	private T prevE1;
	private T prevE2;

	public MergeExceptionIterator(ExceptionIterator<T, E> in1, ExceptionIterator<T, E> in2, Comparator<T> comp) {
		this.in1 = in1;
		this.in2 = in2;
		this.comp = comp;
	}

	@Override
	public boolean hasNext() throws E {
		if (next != null) {
			return true;
		}

		// read next element 1 if required
		if (prevE1 == null && in1.hasNext()) {
			prevE1 = in1.next();
		}
		// read next element 2 if required
		if (prevE2 == null && in2.hasNext()) {
			prevE2 = in2.next();
		}

		if (prevE1 != null && prevE2 != null) {
			// we have an element from both stream, compare them
			if (comp.compare(prevE1, prevE2) < 0) {
				// element 1 lower, return it
				next = prevE1;
				prevE1 = null;
			} else {
				// element 2 lower, return it
				next = prevE2;
				prevE2 = null;
			}
			return true;
		}
		// we have at most one element
		if (prevE1 != null) {
			// return element 1
			next = prevE1;
			prevE1 = null;
			return true;
		}
		if (prevE2 != null) {
			// return element 2
			next = prevE2;
			prevE2 = null;
			return true;
		}
		// nothing else
		return false;
	}

	@Override
	public long getSize() {
		long s1 = in1.getSize();
		long s2 = in2.getSize();
		if (s1 == -1 || s2 == -1) {
			return -1;
		}
		return s2 + s1;
	}

	@Override
	public T next() throws E {
		if (!hasNext()) {
			return null;
		}
		T next = this.next;
		this.next = null;
		return next;
	}
}
