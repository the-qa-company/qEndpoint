package com.the_qa_company.qendpoint.core.iterator.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.function.Function;

public class MergeExceptionParallelIterator<T, E extends Exception> implements ExceptionIterator<T, E> {

	private static final Logger log = LoggerFactory.getLogger(MergeExceptionParallelIterator.class);

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
		return MergeExceptionParallelIterator.buildOfTree(Function.identity(), Comparable::compareTo, array, 0,
				array.size());
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
		return new MergeExceptionParallelIterator<>(buildOfTree(itFunction, comp, array, start, mid),
				buildOfTree(itFunction, comp, array, mid, end), comp);
	}

	private final ExceptionIterator<T, E> in1;
	private final ExceptionIterator<T, E> in2;
	private final Comparator<T> comp;
	private final int chunkSize = 4096;
	private final Executor executor = Executors.newVirtualThreadPerTaskExecutor();

	// Each child's buffered items (at most chunkSize). We'll treat these like
	// queues.
	private final Deque<T> buffer1 = new ArrayDeque<>();
	private final Deque<T> buffer2 = new ArrayDeque<>();

	// Futures for the next chunk fetch (if currently in progress)
	private CompletableFuture<List<T>> future1 = null;
	private CompletableFuture<List<T>> future2 = null;

	public MergeExceptionParallelIterator(ExceptionIterator<T, E> in1, ExceptionIterator<T, E> in2,
			Comparator<T> comp) {
		this.in1 = in1;
		this.in2 = in2;
		this.comp = comp;
	}

	@Override
	public boolean hasNext() throws E {
		// Attempt to ensure we have at least one item available
		prepareNextItem();
		// If both buffers are empty now, we really have no more data
		return !(buffer1.isEmpty() && buffer2.isEmpty());
	}

	@Override
	public T next() throws E {
		if (!hasNext()) {
			return null; // or throw NoSuchElementException
		}
		// We know there's at least one item in buffer1 or buffer2
		T result;
		if (buffer1.isEmpty()) {
			// Must come from buffer2
			result = buffer2.pollFirst();
		} else if (buffer2.isEmpty()) {
			// Must come from buffer1
			result = buffer1.pollFirst();
		} else {
			// Compare the heads
			T head1 = buffer1.peekFirst();
			T head2 = buffer2.peekFirst();
			if (comp.compare(head1, head2) <= 0) {
				result = buffer1.pollFirst();
			} else {
				result = buffer2.pollFirst();
			}
		}
		return result;
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
	 * Ensures at least one buffer is non-empty if data remains. If both are
	 * empty, we fetch from both children in parallel.
	 */
	private void prepareNextItem() throws E {
		// If both buffers are already non-empty, nothing to do
		if (!buffer1.isEmpty() || !buffer2.isEmpty()) {
			return;
		}

		// We may need to start or finish a fetch for each child:
		boolean need1 = buffer1.isEmpty() && in1.hasNext();
		boolean need2 = buffer2.isEmpty() && in2.hasNext();

//		if (need1 && !need2) {
//			if (buffer2.size() < chunkSize / 2 && in2.hasNext()) {
//				need2 = true;
//			}
//		}
//		if (need2 && !need1) {
//			if (buffer1.size() < chunkSize / 2 && in1.hasNext()) {
//				need1 = true;
//			}
//		}

		// If buffer1 is empty and child1 has data, ensure we have a future
		if (need1 && future1 == null) {
			future1 = fetchChunkAsync(in1, chunkSize);
		}
		// If buffer2 is empty and child2 has data, ensure we have a future
		if (need2 && future2 == null) {
			future2 = fetchChunkAsync(in2, chunkSize);
		}

		// If we started any future(s), wait for them all at once
		if (future1 != null || future2 != null) {
			CompletableFuture<?> f1 = (future1 != null) ? future1 : CompletableFuture.completedFuture(null);
			CompletableFuture<?> f2 = (future2 != null) ? future2 : CompletableFuture.completedFuture(null);

			// Wait for both to complete (parallel fetch)
			CompletableFuture.allOf(f1, f2).join();

			// Drain each completed future into its buffer
			if (future1 != null) {
				addToBuffer(future1, buffer1);
				future1 = null;
			}
			if (future2 != null) {
				addToBuffer(future2, buffer2);
				future2 = null;
			}
		}
	}

	/**
	 * Helper to move the fetched chunk from a completed future into the buffer.
	 * Handles exceptions properly.
	 */
	private void addToBuffer(CompletableFuture<List<T>> future, Deque<T> buffer) throws E {
		List<T> chunk;
		try {
			chunk = future.get(); // already done, so non-blocking
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
			throw new RuntimeException("Interrupted while fetching chunk", ie);
		} catch (ExecutionException ee) {
			Throwable cause = ee.getCause();
			if (cause instanceof Exception ex) {
				throw asE(ex);
			} else {
				throw new RuntimeException("Error in parallel chunk fetch", cause);
			}
		}
		chunk.forEach(buffer::addLast);
	}

	/**
	 * Asynchronously fetch up to 'n' items from 'iter' on the executor.
	 */
	private CompletableFuture<List<T>> fetchChunkAsync(ExceptionIterator<T, E> iter, int n) {
		return CompletableFuture.supplyAsync(() -> {
			try {
				return fetchChunk(iter, n);
			} catch (Exception e) {
				throw new CompletionException(e);
			}
		}, executor);
	}

	/**
	 * Synchronous fetch of up to 'n' items.
	 */
	private List<T> fetchChunk(ExceptionIterator<T, E> iter, int n) throws E {
		List<T> chunk = new ArrayList<>(n);
		while (chunk.size() < n && iter.hasNext()) {
			chunk.add(iter.next());
		}
		return chunk;
	}

	@SuppressWarnings("unchecked")
	private E asE(Exception ex) {
		return (E) ex;
	}

}
