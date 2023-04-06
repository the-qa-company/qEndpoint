package com.the_qa_company.qendpoint.core.util.disk;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

public class LongArrayTest {
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

}