package com.the_qa_company.qendpoint.core.unsafe;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UnsafeLongArrayTest {
	private static final int MAX_ARRAY_SIZE = 100;
	private int minMemory;

	@Before
	public void setup() {
		minMemory = MemoryUtils.getMaxArraySize();
		MemoryUtilsTest.setMaxArraySize(MAX_ARRAY_SIZE);
	}

	@After
	public void after() {
		MemoryUtilsTest.setMaxArraySize(minMemory);
	}

	@Test
	public void getSetTest() {
		int len = MAX_ARRAY_SIZE * 10;
		UnsafeLongArray array = UnsafeLongArray.allocate(len);
		assertTrue(array.isUsingUnsafe());
		assertEquals(len, array.length());
		long[] excepted = new long[len];

		Random rnd = new Random(89);
		for (int i = 0; i < len; i++) {
			long l = rnd.nextLong();
			excepted[i] = l;
			array.set(i, l);
		}

		for (int i = 0; i < excepted.length; i++) {
			assertEquals(excepted[i], array.get(i));
		}
	}

	@Test
	public void arrayWrapperTest() {
		int len = MAX_ARRAY_SIZE * 10;
		long[] excepted = new long[len];

		Random rnd = new Random(89);
		for (int i = 0; i < len; i++) {
			long l = rnd.nextLong();
			excepted[i] = l;
		}

		UnsafeLongArray array = UnsafeLongArray.wrapper(excepted);
		assertFalse(array.isUsingUnsafe());
		assertEquals(len, array.length());

		for (int i = 0; i < excepted.length; i++) {
			assertEquals(excepted[i], array.get(i));
		}
	}

	@Test
	public void copyTest() {
		int len = MAX_ARRAY_SIZE * 10;
		long[] excepted = new long[len];

		Random rnd = new Random(89);
		for (int i = 0; i < len; i++) {
			long l = rnd.nextLong();
			excepted[i] = l;
		}

		UnsafeLongArray array = UnsafeLongArray.wrapper(excepted);
		assertFalse(array.isUsingUnsafe());
		assertEquals(len, array.length());
		UnsafeLongArray array2 = UnsafeLongArray.allocate(len);
		assertTrue(array2.isUsingUnsafe());
		assertEquals(len, array2.length());

		UnsafeLongArray.arraycopy(array, 0, array2, 0, len);

		for (int i = 0; i < excepted.length; i++) {
			assertEquals(excepted[i], array.get(i));
		}
		array2.clear();

		for (int i = 0; i < array2.length(); i++) {
			assertEquals("arr[" + i + "/" + array2.length() + "]", 0, array2.get(i));
		}
	}

}
