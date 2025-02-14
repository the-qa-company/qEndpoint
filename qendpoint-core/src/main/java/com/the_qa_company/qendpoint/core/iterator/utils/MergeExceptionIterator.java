package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.function.Function;

public class MergeExceptionIterator<T, E extends Exception> implements ExceptionIterator<T, E> {

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
		int mid = (start + end) / 2;
		return new MergeExceptionIterator<>(buildOfTree(itFunction, comp, array, start, mid),
				buildOfTree(itFunction, comp, array, mid, end), comp);
	}

	private final ExceptionIterator<T, E> in1;
	private final ExceptionIterator<T, E> in2;
	private final Comparator<T> comp;
	private final int chunkSize = 1024 * 4;
	private final Executor executor = Executors.newVirtualThreadPerTaskExecutor(); // Could
	// be
	// a
	// ForkJoinPool.commonPool(),
	// or
	// a
	// custom
	// pool

	private final Deque<T> chunk1 = new ArrayDeque<>();
	private final Deque<T> chunk2 = new ArrayDeque<>();

	// Local buffer to store merged chunks
	private final Deque<T> buffer = new ArrayDeque<>();

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
		if (buffer.isEmpty()) {
			fillBuffer();
		}
		if (buffer.isEmpty()) {
			return false;
		}
		return buffer.peek() != null;
	}

	@Override
	public T next() throws E {
		if (!hasNext()) {
			return null; // or throw NoSuchElementException
		}
		return buffer.pollFirst();
	}

	@Override
	public long getSize() {
		long s1 = in1.getSize();
		long s2 = in2.getSize();
		if (s1 == -1 || s2 == -1) {
			return -1;
		}
		return s1 + s2;
	}

	/**
	 * Fetch a chunk of items from both child iterators in parallel, then merge
	 * them into our local buffer. Only called when the buffer is empty.
	 */
	private void fillBuffer() throws E {

		if (!buffer.isEmpty()) {
			return;
		}

		// Kick off two parallel fetch tasks
		CompletableFuture<Deque<T>> future1 = chunk1.size() < chunkSize && in1.hasNext()
				? fetchChunkAsync(in1, chunk1, chunkSize)
				: null;
		CompletableFuture<Deque<T>> future2 = chunk2.size() < chunkSize && in2.hasNext()
				? fetchChunkAsync(in2, chunk2, chunkSize)
				: null;

		// Wait for both tasks to complete
		if (future1 != null && future2 != null) {
			CompletableFuture.allOf(future1, future2).join();
		}

		Deque<T> chunk1;
		Deque<T> chunk2;
		try {
			chunk1 = future1 != null ? future1.get() : this.chunk1;
			chunk2 = future2 != null ? future2.get() : this.chunk2;
		} catch (InterruptedException ie) {
			// Restore interrupt status
			Thread.currentThread().interrupt();
			throw new RuntimeException("Interrupted while fetching chunks in parallel", ie);
		} catch (ExecutionException ee) {
			// If our underlying fetch threw a checked exception E, unwrap and
			// throw it
			Throwable cause = ee.getCause();
			if (cause instanceof Exception ex) {
				// You may need a different mechanism to re-throw the correct
				// type E
				// e.g. reflection or a wrapper if E is known
				throw asE(ex);
			} else {
				throw new RuntimeException("Unexpected error in parallel fetch", cause);
			}
		}
		if (chunk1.isEmpty()) {
			while (!chunk2.isEmpty()) {
				buffer.addLast(chunk2.pollFirst());
			}
		} else if (chunk2.isEmpty()) {
			while (!chunk1.isEmpty()) {
				buffer.addLast(chunk1.pollFirst());
			}
		} else {
			// Merge the two fetched chunks in sorted order
			mergeChunksIntoBuffer(chunk1, chunk2);
		}
	}

	/**
	 * Helper to schedule a chunk fetch on the given iterator and return a
	 * CompletableFuture. Because T can throw a checked exception E, we wrap the
	 * call and handle exceptions carefully.
	 */
	private CompletableFuture<Deque<T>> fetchChunkAsync(ExceptionIterator<T, E> iter, Deque<T> chunk, int n) {
		CompletableFuture<Deque<T>> future = new CompletableFuture<>();
//		executor.execute(() -> {
		try {
			Deque<T> result = fetchChunk(iter, chunk, n);
			future.complete(result);
		} catch (Exception e) {
			future.completeExceptionally(e);
		}
//		});
		return future;
	}

	/**
	 * Actual synchronous fetch of up to 'n' items from the child iterator.
	 */
	private Deque<T> fetchChunk(ExceptionIterator<T, E> iter, Deque<T> list, int n) throws E {
		while (list.size() < n && iter.hasNext()) {
			list.addLast(iter.next());
		}
		return list;
	}

	/**
	 * Merge two sorted lists into our buffer in ascending order. If the child
	 * iterators are guaranteed sorted, you can do this linear merge. Otherwise,
	 * you'd need a custom approach (possibly sorting the partial chunks).
	 */
	private void mergeChunksIntoBuffer(Deque<T> c1, Deque<T> c2) {

		if (c1.isEmpty() || c2.isEmpty()) {
			return;
		}

		// this assumes that each of the two chunks is sorted
		T c1First = c1.peek();
		T c2Last = c2.peekLast();
		if (comp.compare(c1First, c2Last) > 0) {
			buffer.addAll(c2);
			c2.clear();
			return;
		}

		T c2First = c2.peek();
		T c1Last = c1.peekLast();
		if (comp.compare(c2First, c1Last) > 0) {
			buffer.addAll(c1);
			c1.clear();
			return;
		}

		while (!(c1.isEmpty() || c2.isEmpty())) {
			if (comp.compare(c1.peek(), c2.peek()) < 0) {
				buffer.addLast(c1.pollFirst());
			} else {
				buffer.addLast(c2.pollFirst());
			}
		}
	}

	/**
	 * Utility to cast a generic Exception to E if needed, or wrap as
	 * RuntimeException. Adjust as necessary for your real-world scenario.
	 */
	@SuppressWarnings("unchecked")
	private E asE(Exception ex) {
		return (E) ex;
	}
}
