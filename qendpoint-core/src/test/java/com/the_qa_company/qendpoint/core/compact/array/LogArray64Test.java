package com.the_qa_company.qendpoint.core.compact.array;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import com.the_qa_company.qendpoint.core.util.BitUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64;

@RunWith(Parameterized.class)
public class LogArray64Test {

	private final int numbits;
	private static final int numentries = 10000;
	SequenceLog64 array;
	long[] plain;

	@Parameters
	public static Collection<Object[]> data() {
		Object[][] selected = new Object[][] { { 1 }, { 3 }, { 8 }, { 9 }, { 10 }, { 15 }, { 22 }, { 25 }, { 30 },
				{ 32 }, { 40 }, { 48 }, { 60 } };
		return Arrays.asList(selected);
	}

	public LogArray64Test(int numbits) {
		this.numbits = numbits;
	}

	@Before
	public void setUp() throws Exception {
		Random r = new Random(1);

		plain = new long[numentries];

		long max = BitUtil.maxVal(numbits);
		array = new SequenceLog64(numbits, numentries);

		for (int k = 0; k < numentries; k++) {
			long value = Math.abs(r.nextLong()) % max;
			array.append(value);
			plain[k] = value;
		}
	}

	private void testArraysEqual() {
		for (int i = 0; i < numentries; i++) {
			// System.out.println("\t "+i+" => Value1: "+arrays.get(i) + "
			// Value2: "+plain[i]);
			assertEquals("Different value get " + numbits, array.get(i), plain[i]);
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
			long maxValue = BitUtil.maxVal(numbits);

			for (int k = 0; k < nummodifications; k++) {

				int index = Math.abs(r.nextInt()) % maxKey;
				long value = Math.abs(r.nextLong()) % maxValue;

				array.set(index, value);
				plain[index] = value;
			}
		}

		testArraysEqual();
	}

	@Test
	public void testGetNumberOfElements() {
		assertEquals("Different Size " + numbits, array.getNumberOfElements(), numentries);
	}

	@Test
	public void testLoadSave() {
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			array.save(out, null);

			ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());

			try (SequenceLog64 loaded = new SequenceLog64()) {
				loaded.load(in, null);

				assertEquals("Save/Load different number of elements", array.getNumberOfElements(),
						loaded.getNumberOfElements());
				assertEquals("Save/Load different number of bits", array.getNumBits(), loaded.getNumBits());
				for (int i = 0; i < array.getNumberOfElements(); i++) {
					assertEquals("Save/Load different value", array.get(i), loaded.get(i));
				}
			}
		} catch (IOException e) {
			fail("Exception thrown: " + e);
		}
	}

}
