package com.the_qa_company.qendpoint.core.compact.sequence;

import com.the_qa_company.qendpoint.core.storage.QEPMapIdSorter;
import com.the_qa_company.qendpoint.core.unsafe.MemoryUtils;
import com.the_qa_company.qendpoint.core.unsafe.MemoryUtilsTest;
import com.the_qa_company.qendpoint.core.util.disk.LongArray;
import com.the_qa_company.qendpoint.core.util.disk.SimpleLongArray;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import org.apache.commons.io.file.PathUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class LargeArrayTest {

	public static void assertSorted(LongArray array, boolean allowDupes) {
		assertSorted(array, Long::compareTo, allowDupes);
	}

	public static void assertSortedDesc(LongArray array, boolean allowDupes) {
		assertSorted(array, ((Comparator<Long>) Long::compareTo).reversed(), allowDupes);
	}

	public static void assertSorted(LongArray array, Comparator<Long> comparator, boolean allowDupes) {
		if (array.isEmpty()) {
			return;
		}

		long last = array.get(0);

		if (allowDupes) {
			// no dupe check
			for (int i = 1; i < array.length(); i++) {
				long v = array.get(i);

				if (comparator.compare(last, v) > 0) {
					throw new AssertionError("Unordered element: " + last + " > " + v + " at index " + i);
				}
			}
			return;
		}
		// with dupe check
		for (int i = 1; i < array.length(); i++) {
			long v = array.get(i);

			if (comparator.compare(last, v) >= 0) {
				throw new AssertionError("Unordered element: " + last + " >= " + v + " at index " + i);
			}
		}
	}

	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void allocationTest() {
		int old = MemoryUtils.getMaxArraySize();
		try {
			MemoryUtilsTest.setMaxArraySize(100);
			long size = MemoryUtils.getMaxArraySize() + 2L;
			IOUtil.createLargeArray(size, false);
		} finally {
			MemoryUtilsTest.setMaxArraySize(old);
		}
	}

	@Test
	public void binarySearchTest() {
		LongArray arr = SimpleLongArray.wrapper(new long[]{0, 1, 2, 3, 4, 5, 6, 7, 8});

		for (int i = 0; i < arr.length(); i++) {
			assertEquals(i, arr.binarySearch(i));
		}

		assertEquals(-1, arr.binarySearch(-2));
		assertEquals(-1, arr.binarySearch(-9));
		assertEquals(-1, arr.binarySearch(9));

		LongArray arr2 = SimpleLongArray.wrapper(new long[]{4, 8, 12, 16, 20});

		for (int i = 0; i < arr2.length(); i++) {
			assertEquals(i, arr2.binarySearch(arr2.get(i)));
		}

		Random rnd = new Random(82);
		LongArray arr3 = SimpleLongArray.wrapper(rnd.longs().limit(100).sorted().toArray());

		for (int i = 0; i < arr3.length(); i++) {
			assertEquals(i, arr3.binarySearch(arr3.get(i)));
		}
	}

	@Test
	public void linearSearchTest() {
		Random rnd = new Random(82);
		LongArray arr = SimpleLongArray.wrapper(rnd.longs().limit(100).toArray());

		for (int i = 0; i < arr.length(); i++) {
			assertEquals(i, arr.linearSearch(arr.get(i)));
		}
	}

	@Test
	public void simpleBinaryMapping() throws IOException {
		Path root = tempDir.newFolder().toPath();
		final long seed = 34;
		final long size = 10_000;
		LongArray[] map1Id = {LongArray.of(size), LongArray.of(size)};
		LongArray[] map1Perm = {LongArray.of(size), LongArray.of(size)};
		LongArray[] map2Id = {LongArray.of(size), LongArray.of(size)};
		LongArray[] map2Perm = {LongArray.of(size), LongArray.of(size)};


		try (
				QEPMapIdSorter sorterS1 = new QEPMapIdSorter(root.resolve("sorterS1"), size, Integer.MAX_VALUE);
				QEPMapIdSorter sorterS2 = new QEPMapIdSorter(root.resolve("sorterS2"), size, Integer.MAX_VALUE);
				QEPMapIdSorter sorterO1 = new QEPMapIdSorter(root.resolve("sorterO1"), size, Integer.MAX_VALUE);
				QEPMapIdSorter sorterO2 = new QEPMapIdSorter(root.resolve("sorterO2"), size, Integer.MAX_VALUE)
		) {
			Random rnd = new Random(seed);

			for (int i = 0; i < size; i++) {
				long v1 = rnd.nextInt(Integer.MAX_VALUE);
				long v2 = rnd.nextInt(Integer.MAX_VALUE);

				if ((v1 & 1) == 0) {
					// v1 subject
					sorterS1.addElement(v1 >> 1, v2);
				} else {
					// v1 object
					sorterO1.addElement(v1 >> 1, v2);
				}

				if ((v2 & 1) == 0) {
					// v2 subject
					sorterS2.addElement(v2 >> 1, v1);
				} else {
					// v2 object
					sorterO2.addElement(v2 >> 1, v1);
				}
			}

			sorterS1.sort();
			sorterS2.sort();
			sorterO1.sort();
			sorterO2.sort();

			for (int i = 0; i < 2; i++) {
				QEPMapIdSorter sorterS = (i & 1) == 0 ? sorterS1 : sorterS2;
				QEPMapIdSorter sorterO = (i & 1) == 0 ? sorterO1 : sorterO2;
				long index = 0;
				for (QEPMapIdSorter.QEPMapIds ids : sorterS) {
					map1Id[i].set(index, ids.origin());
					map1Perm[i].set(index, ids.destination());
					index++;
				}
				map1Id[i].resize(index);
				map1Perm[i].resize(index);
				index = 0;
				for (QEPMapIdSorter.QEPMapIds ids : sorterO) {
					map2Id[i].set(index, ids.origin());
					map2Perm[i].set(index, ids.destination());
					index++;
				}
				map2Id[i].resize(index);
				map2Perm[i].resize(index);
			}

			for (int i = 0; i < 2; i++) {
				assertSorted(map1Id[i], true);
				assertSorted(map2Id[i], true);
			}


			Random rnd2 = new Random(seed);

			for (int i = 0; i < size; i++) {
				long v1 = rnd2.nextInt(Integer.MAX_VALUE);
				long v2 = rnd2.nextInt(Integer.MAX_VALUE);

				if ((v1 & 1) == 0) {
					// v1 subject
					long location = map1Id[0].binarySearch(v1 >> 1);
					assertNotEquals(location, -1);

					assertEquals(map1Id[0].get(location), v1 >> 1);
					assertEquals(map1Perm[0].get(location), v2);
				} else {
					// v1 object
					long location = map2Id[0].binarySearch(v1 >> 1);
					assertNotEquals(location, -1);

					assertEquals(map2Id[0].get(location), v1 >> 1);
					assertEquals(map2Perm[0].get(location), v2);
				}

				if ((v2 & 1) == 0) {
					// v2 subject
					long location = map1Id[1].binarySearch(v2 >> 1);
					assertNotEquals(location, -1);

					assertEquals(map1Id[1].get(location), v2 >> 1);
					assertEquals(map1Perm[1].get(location), v1);
				} else {
					// v2 object
					long location = map2Id[1].binarySearch(v2 >> 1);
					assertNotEquals(location, -1);

					assertEquals(map2Id[1].get(location), v2 >> 1);
					assertEquals(map2Perm[1].get(location), v1);
				}
			}
		} finally {
			PathUtils.deleteDirectory(root);
		}
	}
}
