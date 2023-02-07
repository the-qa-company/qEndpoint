package com.the_qa_company.qendpoint.core.iterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import com.the_qa_company.qendpoint.core.iterator.utils.MergedIterator;

import static org.junit.Assert.*;

public class MergedIteratorTest {
	List<Integer> listA, listB, listC;

	private List<Integer> getList(int[] ints) {
		List<Integer> intList = new ArrayList<>();
		for (int anInt : ints) {
			intList.add(anInt);
		}
		return intList;
	}

	@Before
	public void setUp() throws Exception {

		int[] intsA = { 1, 4, 6 };
		int[] intsB = { 3, 4, 5 };
		Integer[] intsExpected = { 1, 3, 4, 5, 6 };

		listA = getList(intsA);
		listB = getList(intsB);
		listC = Arrays.asList(intsExpected);
	}

	@Test
	public void testOneEmpty() {

		Iterator<Integer> it = new MergedIterator<>(listA.iterator(), listB.iterator(), Comparator.reverseOrder());

		Iterator<Integer> itE = listC.iterator();

		while (it.hasNext()) {
			assertTrue(itE.hasNext());

			int val = it.next();
			int valE = itE.next();
//			System.out.println(val);
			assertEquals(val, valE);
		}

		assertFalse(itE.hasNext());
	}

}
