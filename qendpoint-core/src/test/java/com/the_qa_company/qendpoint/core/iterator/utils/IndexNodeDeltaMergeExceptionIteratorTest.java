package com.the_qa_company.qendpoint.core.iterator.utils;

import com.the_qa_company.qendpoint.core.triples.IndexedNode;
import com.the_qa_company.qendpoint.core.util.string.AssertionCharSequence;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.CompactString;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.the_qa_company.qendpoint.core.iterator.utils.IndexNodeDeltaMergeExceptionIterator.compareToDelta;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class IndexNodeDeltaMergeExceptionIteratorTest {

	@Test
	public void compareToDeltaTest() {
		assertEquals(3, compareToDelta(2, new IndexedNode("aaa", 0), new IndexedNode("aab", 1)));
		assertEquals(-3, compareToDelta(2, new IndexedNode("aab", 1), new IndexedNode("aaa", 0)));
		assertEquals(3, compareToDelta(2, new IndexedNode("aaad", 0), new IndexedNode("aabd", 1)));
		assertEquals(-3, compareToDelta(2, new IndexedNode("aabd", 1), new IndexedNode("aaad", 0)));
		assertEquals(4, compareToDelta(2, new IndexedNode("aaaad", 0), new IndexedNode("aaabd", 1)));
		assertEquals(-4, compareToDelta(2, new IndexedNode("aaabd", 1), new IndexedNode("aaaad", 0)));

		assertEquals(3, compareToDelta(0, new IndexedNode("aa", 1), new IndexedNode("aa", 0)));
	}

	private final AtomicLong ids = new AtomicLong();

	public IndexNodeDeltaMergeExceptionIterator.IndexNodeDeltaFetcher<RuntimeException> createFromSortedArray(
			AssertionCharSequence... array) {
		List<IndexedNode> elements = Stream.of(array).map(s -> new IndexedNode(s, ids.incrementAndGet()))
				.collect(Collectors.toList());

		return new IndexNodeDeltaMergeExceptionIterator.IndexNodeDeltaFetcher<>() {
			int index = 0;
			int delta;

			@Override
			public IndexedNode fetchNode() throws RuntimeException {
				if (elements.size() <= index) {
					return null;
				}

				IndexedNode next = elements.get(index++);

				delta = 0;

				if (index > 1) {
					IndexedNode prev = elements.get(index - 2);
					CharSequence s1 = ((AssertionCharSequence) prev.getNode()).getSequence();
					CharSequence s2 = ((AssertionCharSequence) next.getNode()).getSequence();

					delta = 0;
					int len = Math.min(s1.length(), s2.length());
					while (delta < len && s1.charAt(delta) == s2.charAt(delta)) {
						delta++;
					}
				}
				return next;
			}

			@Override
			public int lastDelta() throws RuntimeException {
				return delta;
			}
		};
	}

	@Test
	public void deltaComputeTest() {
		IndexNodeDeltaMergeExceptionIterator.IndexNodeDeltaFetcher<RuntimeException> it = createFromSortedArray(
				new AssertionCharSequence("", 0), new AssertionCharSequence("", 0), new AssertionCharSequence("aaa", 0),
				new AssertionCharSequence("aab", 0), new AssertionCharSequence("aac", 0),
				new AssertionCharSequence("aacd", 0), new AssertionCharSequence("abcd", 0),
				new AssertionCharSequence("bbcd", 0));

		assertEquals(CompactString.EMPTY, ((AssertionCharSequence) it.fetchNode().getNode()).getSequence());
		assertEquals(0, it.lastDelta());

		assertEquals(CompactString.EMPTY, ((AssertionCharSequence) it.fetchNode().getNode()).getSequence());
		assertEquals(0, it.lastDelta());

		assertEquals(new CompactString("aaa"), ((AssertionCharSequence) it.fetchNode().getNode()).getSequence());
		assertEquals(0, it.lastDelta());

		assertEquals(new CompactString("aab"), ((AssertionCharSequence) it.fetchNode().getNode()).getSequence());
		assertEquals(2, it.lastDelta());

		assertEquals(new CompactString("aac"), ((AssertionCharSequence) it.fetchNode().getNode()).getSequence());
		assertEquals(2, it.lastDelta());

		assertEquals(new CompactString("aacd"), ((AssertionCharSequence) it.fetchNode().getNode()).getSequence());
		assertEquals(3, it.lastDelta());

		assertEquals(new CompactString("abcd"), ((AssertionCharSequence) it.fetchNode().getNode()).getSequence());
		assertEquals(1, it.lastDelta());

		assertEquals(new CompactString("bbcd"), ((AssertionCharSequence) it.fetchNode().getNode()).getSequence());
		assertEquals(0, it.lastDelta());

		assertNull(it.fetchNode());
	}

	@Test
	public void mergeComputeTest() {
		List<ByteString> output = Stream.of("", "a", "b").map(ByteString::of).collect(Collectors.toList());
		IndexNodeDeltaMergeExceptionIterator.IndexNodeDeltaFetcher<RuntimeException> it1 = createFromSortedArray(
				new AssertionCharSequence("a", 0));
		IndexNodeDeltaMergeExceptionIterator.IndexNodeDeltaFetcher<RuntimeException> it2 = createFromSortedArray(
				new AssertionCharSequence("", 0), new AssertionCharSequence("b", 0));

		ExceptionIterator<IndexedNode, RuntimeException> it = IndexNodeDeltaMergeExceptionIterator
				.buildOfTree(List.of(it1, it2), 0, 2);

		Iterator<ByteString> itE = output.iterator();
		while (it.hasNext()) {
			CharSequence sequence = ((AssertionCharSequence) it.next().getNode()).getSequence();
			assertTrue("missing: " + sequence, itE.hasNext());
			assertEquals(itE.next(), sequence);
		}
		assertFalse(it.hasNext());
	}

	@Test
	public void mergeCountComputeTest() {
		List<ByteString> output = Stream
				.of("", "aaa", "aab", "aabb", "aacd", "aacde", "bacd", "bacdd", "bacde", "bacdz").map(ByteString::of)
				.collect(Collectors.toList());
		IndexNodeDeltaMergeExceptionIterator.IndexNodeDeltaFetcher<RuntimeException> it1 = createFromSortedArray(
				new AssertionCharSequence("aaa", 0), new AssertionCharSequence("aacde", 0),
				new AssertionCharSequence("bacd", 0), new AssertionCharSequence("bacdd", 0));
		IndexNodeDeltaMergeExceptionIterator.IndexNodeDeltaFetcher<RuntimeException> it2 = createFromSortedArray(
				new AssertionCharSequence("", 0), new AssertionCharSequence("aab", 0),
				new AssertionCharSequence("aabb", 0), new AssertionCharSequence("aacd", 0),
				new AssertionCharSequence("bacde", 0), new AssertionCharSequence("bacdz", 0));

		ExceptionIterator<IndexedNode, RuntimeException> it = IndexNodeDeltaMergeExceptionIterator
				.buildOfTree(List.of(it1, it2), 0, 2);

		Iterator<ByteString> itE = output.iterator();
		while (it.hasNext()) {
			CharSequence sequence = ((AssertionCharSequence) it.next().getNode()).getSequence();
			assertTrue("missing: " + sequence, itE.hasNext());
			assertEquals(itE.next(), sequence);
		}
		assertFalse(itE.hasNext());
	}

	@Test
	public void deepMergeComputeTest() {
		List<ByteString> output = Stream.of("", "aa", "aaa", "aaa", "aab", "aabb", "aabb", "aacc", "aacd", "aacde",
				"aacdef", "b", "bcd", "bcz", "bz", "bze", "cd", "ce").map(ByteString::of).collect(Collectors.toList());
		IndexNodeDeltaMergeExceptionIterator.IndexNodeDeltaFetcher<RuntimeException> it1 = createFromSortedArray(
				new AssertionCharSequence("aa", 0), new AssertionCharSequence("aaa", 0),
				new AssertionCharSequence("aaa", 0), new AssertionCharSequence("aabb", 0),
				new AssertionCharSequence("aacc", 0));
		IndexNodeDeltaMergeExceptionIterator.IndexNodeDeltaFetcher<RuntimeException> it2 = createFromSortedArray(
				new AssertionCharSequence("aab", 0), new AssertionCharSequence("aabb", 0),
				new AssertionCharSequence("aacd", 0), new AssertionCharSequence("b", 0),
				new AssertionCharSequence("bcd", 0), new AssertionCharSequence("bcz", 0),
				new AssertionCharSequence("bz", 0), new AssertionCharSequence("bze", 0));
		IndexNodeDeltaMergeExceptionIterator.IndexNodeDeltaFetcher<RuntimeException> it3 = createFromSortedArray(
				new AssertionCharSequence("", 0), new AssertionCharSequence("aacde", 0));
		IndexNodeDeltaMergeExceptionIterator.IndexNodeDeltaFetcher<RuntimeException> it4 = createFromSortedArray(
				new AssertionCharSequence("cd", 0), new AssertionCharSequence("ce", 0));
		IndexNodeDeltaMergeExceptionIterator.IndexNodeDeltaFetcher<RuntimeException> it5 = createFromSortedArray(
				new AssertionCharSequence("aacdef", 0));

		ExceptionIterator<IndexedNode, RuntimeException> it = IndexNodeDeltaMergeExceptionIterator
				.buildOfTree(List.of(it1, it2, it3, it4, it5), 0, 5);

		((IndexNodeDeltaMergeExceptionIterator<?>) it).printMergeTree();

		Iterator<ByteString> itE = output.iterator();
		while (it.hasNext()) {
			assertTrue(itE.hasNext());
			ByteString seq = ((AssertionCharSequence) it.next().getNode()).getSequence();
			assertEquals(itE.next(), seq);
		}
		assertFalse(itE.hasNext());
	}

	@Test
	public void largeTest() {
		// (tried with 200_000)
		final long size = 2_000;
		Random random = new Random(35);
		List<ByteString> randy = Stream.generate(() -> {
			String table = "abcd";
			StringBuilder bld = new StringBuilder();
			// +1 because we don't have empty strings during this step
			int bn = 1 + random.nextInt(20);
			while (bn > 0) {
				bld.append(table.charAt(bn % table.length()));
				bn /= table.length();
			}
			return new CompactString(bld);
		}).limit(size).collect(Collectors.toList());
		List<ByteString> sortedRandy = randy.stream().sorted().collect(Collectors.toList());

		assertEquals(size, sortedRandy.size());

		ExceptionIterator<IndexedNode, RuntimeException> it = IndexNodeDeltaMergeExceptionIterator
				.buildOfTree(s -> createFromSortedArray(new AssertionCharSequence(s, 0)), randy, 0, randy.size());

		int index = 0;
		Iterator<ByteString> itE = sortedRandy.iterator();
		while (it.hasNext()) {
			assertTrue(itE.hasNext());
			CharSequence actual = ((AssertionCharSequence) it.next().getNode()).getSequence();
			assertEquals("Difference with index #" + index++, itE.next(), actual);
		}
		assertFalse(itE.hasNext());

	}
}
