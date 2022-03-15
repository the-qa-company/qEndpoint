package com.the_qa_company.q_endpoint;

import com.the_qa_company.q_endpoint.utils.BitArrayDisk;

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
    public void testInit() {
        try {
            BitArrayDisk bitArrayDisk = new BitArrayDisk(100, tempDir.newFile("triples-delete.arr"));
            // expect 2 words of 64 bits to represent 100 bits
            assertEquals(2, bitArrayDisk.getNumWords());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSetValues() {
        try {
            BitArrayDisk bitArrayDisk = new BitArrayDisk(100, tempDir.newFile("triples-delete.arr"));
            bitArrayDisk.set(99, true);
            assertTrue(bitArrayDisk.access(99));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testReinitialize() {
        try {
            File file = tempDir.newFile("triples-delete.arr");
            BitArrayDisk bitArrayDisk = new BitArrayDisk(100, file);
            bitArrayDisk.set(99, true);
            bitArrayDisk.close();

            // should read content from disk
            bitArrayDisk = new BitArrayDisk(100, file);
            assertTrue(bitArrayDisk.access(99));
            assertEquals(2, bitArrayDisk.getNumWords());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCountOnes() {
        try {
            BitArrayDisk bitArrayDisk = new BitArrayDisk(100000, tempDir.newFile("triples-delete.arr"));

            for (int i = 0; i < 50000; i++) {
                bitArrayDisk.set(i, true);
            }
            for (int i = 50000; i < 60000; i++) {
                bitArrayDisk.set(i, true);
            }

            assertEquals(60000, bitArrayDisk.countOnes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private int log2(long value) {
        if (value == 0) {
            return 0; // Wrong, but it's private
        }
        long v = value;
        int log = 0;

        while (v != 0) {
            v >>>= 1;
            log++;
        }

        return log;
    }

    @Test
    public void testLog2() {
        Assert.assertEquals(64, log2(-1));
        for (int i = 0; i < 64; i++) {
            Assert.assertEquals(i + 1, log2(1L << i));
        }

        Assert.assertEquals(64, log2(-42L));
    }
}
