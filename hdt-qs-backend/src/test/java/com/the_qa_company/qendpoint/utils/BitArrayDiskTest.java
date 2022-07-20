package com.the_qa_company.qendpoint.utils;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BitArrayDiskTest {

	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	@Test
	public void testInit() throws IOException {
		try (BitArrayDisk bitArrayDisk = new BitArrayDisk(100, tempDir.newFile("triples-delete.arr"))){
			// expect 2 words of 64 bits to represent 100 bits
			assertEquals(2, bitArrayDisk.getNumWords());
		}
	}

	@Test
	public void testSetValues() throws IOException {
		try (BitArrayDisk bitArrayDisk = new BitArrayDisk(100, tempDir.newFile("triples-delete.arr"))) {
			bitArrayDisk.set(99, true);
			assertTrue(bitArrayDisk.access(99));
		}
	}

	@Test
	public void testReinitialize() throws IOException {
		File file = tempDir.newFile("triples-delete.arr");

		try (BitArrayDisk bitArrayDisk = new BitArrayDisk(100, file)) {
			bitArrayDisk.set(99, true);
		}

		// should read content from disk
		try (BitArrayDisk bitArrayDisk = new BitArrayDisk(100, file)) {
			assertTrue(bitArrayDisk.access(99));
			assertEquals(2, bitArrayDisk.getNumWords());
		}
	}

	@Test
	public void testCountOnes() throws IOException {
		try (BitArrayDisk bitArrayDisk = new BitArrayDisk(100000, tempDir.newFile("triples-delete.arr"))) {
			for (int i = 0; i < 50000; i++) {
				bitArrayDisk.set(i, true);
			}
			for (int i = 50000; i < 60000; i++) {
				bitArrayDisk.set(i, true);
			}

			assertEquals(60000, bitArrayDisk.countOnes());
		}
	}

	@Test
	public void testLog2() {
		Assert.assertEquals(64, BitArrayDisk.log2(-1));
		for (int i = 0; i < 64; i++) {
			Assert.assertEquals(i + 1, BitArrayDisk.log2(1L << i));
		}

		Assert.assertEquals(64, BitArrayDisk.log2(-42L));
	}
}
