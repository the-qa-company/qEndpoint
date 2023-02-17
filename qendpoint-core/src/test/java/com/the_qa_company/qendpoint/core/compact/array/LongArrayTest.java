package com.the_qa_company.qendpoint.core.compact.array;

import com.the_qa_company.qendpoint.core.compact.sequence.SequenceInt64;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class LongArrayTest {

	private static final int numentries = 10000;
	SequenceInt64 array;
	long[] plain;

	@Before
	public void setUp() throws Exception {
		Random r = new Random(1);

		plain = new long[numentries];
		array = new SequenceInt64(numentries);

		for (int k = 0; k < numentries; k++) {
			long value = Math.abs(r.nextLong());
			array.append(value);
			plain[k] = value;
		}
	}

	@Test
	public void testArraysEqual() {
		for (int i = 0; i < numentries; i++) {
			// System.out.println("\t "+i+" => Value1: "+arrays.get(i) + "
			// Value2: "+plain[i]);
			assertEquals("Different value on position " + i, array.get(i), plain[i]);
		}
	}

	@Test
	public void testGet() {
		testArraysEqual();
	}

	@Test
	public void testSet() {
		Random r = new Random(10);
		int nummodifications = Math.max(10, numentries / 10);

		// Modify 10% of the buckets with random values.
		for (int i = 0; i < nummodifications; i++) {
			int maxKey = (int) array.getNumberOfElements();

			for (int k = 0; k < nummodifications; k++) {

				int index = Math.abs(r.nextInt()) % maxKey;
				long value = Math.abs(r.nextLong());

				array.set(index, value);
				plain[index] = value;
			}
		}

		testArraysEqual();
	}

	@Test
	public void testGetNumberOfElements() {
		assertEquals("Different Size ", array.getNumberOfElements(), numentries);
	}

	@Test
	public void testLoadSave() {
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			array.save(out, null);

			ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());

			SequenceInt64 loaded = new SequenceInt64();
			loaded.load(in, null);

			assertEquals("Save/Load different number of elements", array.getNumberOfElements(),
					loaded.getNumberOfElements());
			for (int i = 0; i < array.getNumberOfElements(); i++) {
				assertEquals("Save/Load different value", array.get(i), loaded.get(i));
			}
		} catch (IOException e) {
			fail("Exception thrown: " + e);
		}
	}

}
