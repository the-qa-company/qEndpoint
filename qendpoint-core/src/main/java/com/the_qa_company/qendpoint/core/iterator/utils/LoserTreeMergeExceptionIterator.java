package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Optimized K-way merge using a Loser Tree.
 * <p>
 * Optimizations: 1. Removed 'active' array (reduced cache pressure). Uses a
 * SENTINEL object for exhausted streams. 2. Manually inlined comparison logic
 * in hot paths (replayGames) to eliminate method call overhead. 3. Hoisted
 * array references to local variables to prevent heap field dereferencing in
 * loops. 4. Flattened control flow for better branch prediction.
 */
public class LoserTreeMergeExceptionIterator<T, E extends Exception> implements ExceptionIterator<T, E> {

	// Sentinel object to mark an exhausted iterator.
	// We use this instead of a boolean[] active array to reduce memory lookups.
	private static final Object SENTINEL = new Object();

	public static <T, E extends Exception> ExceptionIterator<T, E> merge(
			List<? extends ExceptionIterator<T, E>> sources, Comparator<? super T> comparator) {
		return new LoserTreeMergeExceptionIterator<>(sources, comparator);
	}

	public static <T extends Comparable<? super T>, E extends Exception> ExceptionIterator<T, E> merge(
			List<? extends ExceptionIterator<T, E>> sources) {
		return merge(sources, Comparable::compareTo);
	}

	private final List<? extends ExceptionIterator<T, E>> sources;
	private final Comparator<? super T> comparator;
	private final long size;

	private boolean initialized;
	private int leafStart;
	private ExceptionIterator<T, E>[] iterators;

	// Flattened tree arrays
	private int[] tree;
	private Object[] values; // Contains T or SENTINEL

	// State tracking
	private int pendingReturnLeaf = -1;
	private T next;

	public LoserTreeMergeExceptionIterator(List<? extends ExceptionIterator<T, E>> sources,
			Comparator<? super T> comparator) {
		this.sources = sources == null ? List.of() : sources;
		this.comparator = Objects.requireNonNull(comparator, "comparator");
		this.size = computeSize(this.sources);
	}

	@Override
	public boolean hasNext() throws E {
		if (next != null) {
			return true;
		}

		if (Thread.currentThread().isInterrupted()) {
			return false;
		}

		// Fast-path: check initialization
		if (!initialized) {
			initialize();
		}

		// If tree is empty, we are done
		if (tree.length == 0) {
			return false;
		}

		// If we returned a value previously, we must advance that specific leaf
		// and let it compete in the tournament again.
		if (pendingReturnLeaf != -1) {
			advanceAndReplay(pendingReturnLeaf);
			pendingReturnLeaf = -1;
		}

		// The winner of the tournament is always at tree[0]
		int winnerLeaf = tree[0];

		// Check if the overall winner is actually the SENTINEL (all streams
		// exhausted)
		Object val = values[winnerLeaf];
		if (val == SENTINEL) {
			return false;
		}

		pendingReturnLeaf = winnerLeaf;
		// noinspection unchecked
		next = (T) val;
		return true;
	}

	@Override
	public T next() throws E {
		if (!hasNext()) {
			return null;
		}
		T value = next;
		next = null;
		return value;
	}

	@Override
	public long getSize() {
		return size;
	}

	private void initialize() throws E {
		initialized = true;
		leafStart = sources.size();

		if (leafStart == 0) {
			iterators = emptyIterators(0);
			tree = new int[0];
			values = new Object[0];
			return;
		}

		iterators = emptyIterators(leafStart);
		// Tree size is 2k. Indices 0..k-1 are internal nodes, k..2k-1 are
		// leaves.
		int arraySize = leafStart * 2;
		tree = new int[arraySize];
		values = new Object[arraySize];

		// 1. Populate Leaves
		for (int i = 0; i < leafStart; i++) {
			ExceptionIterator<T, E> iterator = sources.get(i);
			iterators[i] = iterator;
			int leafIndex = leafStart + i;

			if (iterator != null && iterator.hasNext()) {
				values[leafIndex] = iterator.next();
			} else {
				values[leafIndex] = SENTINEL;
			}
		}

		// 2. Build the Tournament Tree
		// 'winners' is a temporary array needed only during initialization
		// to track who moves up.
		int[] winners = new int[arraySize];
		for (int i = leafStart; i < arraySize; i++) {
			winners[i] = i;
		}

		// Cache fields for the loop
		final Object[] vals = this.values;
		final Comparator<? super T> comp = this.comparator;
		final int[] t = this.tree;

		// Play matches from back to front
		for (int i = arraySize - 2; i > 0; i -= 2) {
			int right = winners[i + 1];
			int left = winners[i];

			int winner, loser;

			// Inlined Comparison Logic (Left vs Right)
			// We want the 'smaller' value to be the winner.
			Object vLeft = vals[left];
			Object vRight = vals[right];

			boolean leftWins;
			if (vLeft == SENTINEL) {
				leftWins = false; // Left is exhausted, Right wins (even if
									// Right is also SENTINEL, doesn't matter)
			} else if (vRight == SENTINEL) {
				leftWins = true; // Right exhausted, Left wins
			} else {
				// Both are active, use Comparator
				// noinspection unchecked
				int c = comp.compare((T) vLeft, (T) vRight);
				if (c != 0) {
					leftWins = c < 0;
				} else {
					// Stability: lower index wins ties
					leftWins = left < right;
				}
			}

			if (leftWins) {
				winner = left;
				loser = right;
			} else {
				winner = right;
				loser = left;
			}

			int parent = i >>> 1;
			t[parent] = loser; // Internal node stores the loser
			winners[parent] = winner; // Winner advances
		}

		t[0] = winners[1];
	}

	/**
	 * Advances the specific leaf and replays the game up the tree. Combined
	 * method to reduce overhead.
	 */
	private void advanceAndReplay(int leaf) throws E {
		// 1. Advance the iterator for this leaf
		int sourceIndex = leaf - leafStart;
		ExceptionIterator<T, E> it = iterators[sourceIndex];

		if (it.hasNext()) {
			values[leaf] = it.next();
		} else {
			values[leaf] = SENTINEL;
		}

		// 2. Replay Games (Hot Path)
		// We lift fields to local variables for speed (Register allocation)
		final int[] t = this.tree;
		final Object[] vals = this.values;
		final Comparator<? super T> comp = this.comparator;

		int currentWinner = leaf;
		Object currentVal = vals[leaf];

		// Traverse from leaf to root
		for (int node = leaf >>> 1; node != 0; node >>>= 1) {
			int challenger = t[node]; // The loser stored at this node

			// Optimization: If the current winner is SENTINEL, it loses against
			// everyone
			// (except another SENTINEL, but logic handles that naturally).
			// We can skip comparison logic if we know the result is fixed.
			if (currentVal == SENTINEL) {
				// Current is exhausted (Infinity). It loses.
				// The challenger wins and moves up. We stay as the loser.
				t[node] = currentWinner;
				currentWinner = challenger;
				currentVal = vals[challenger];
				continue;
			}

			Object challengerVal = vals[challenger];

			// If challenger is SENTINEL, Current wins automatically.
			if (challengerVal == SENTINEL) {
				// Current wins. Challenger stays as loser.
				// Loop continues with currentWinner unchanged.
				continue;
			}

			// Both are active. Compare.
			// Check if Challenger is BETTER than Current.
			// If Challenger < Current, Challenger wins.
			// noinspection unchecked
			int c = comp.compare((T) challengerVal, (T) currentVal);

			boolean challengerWins = false;
			if (c < 0) {
				challengerWins = true;
			} else if (c == 0) {
				// Stability check: Lower index wins.
				// If Challenger index < Current index, Challenger wins.
				challengerWins = challenger < currentWinner;
			}

			if (challengerWins) {
				// Challenger wins.
				// Current winner becomes the loser and is stored at this node.
				t[node] = currentWinner;

				// Challenger becomes the new winner bubbling up.
				currentWinner = challenger;
				currentVal = challengerVal;
			}
			// Else: Current wins, Challenger stays as loser. Loop continues.
		}

		t[0] = currentWinner;
	}

	@SuppressWarnings("unchecked")
	private ExceptionIterator<T, E>[] emptyIterators(int size) {
		return (ExceptionIterator<T, E>[]) new ExceptionIterator[size];
	}

	private static <T, E extends Exception> long computeSize(List<? extends ExceptionIterator<T, E>> sources) {
		long size = 0;
		for (ExceptionIterator<T, E> source : sources) {
			if (source == null)
				continue;
			long s = source.getSize();
			if (s == -1)
				return -1;
			size += s;
		}
		return size;
	}
}
