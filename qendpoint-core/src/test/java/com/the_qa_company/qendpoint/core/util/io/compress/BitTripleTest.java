package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.enums.CompressionType;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.util.BitUtil;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

public class BitTripleTest {
	@Test
	public void dataTest() throws IOException {
		ByteArrayOutputStream os = new ByteArrayOutputStream();

		final int maxY = 12345;
		final int maxZ = 1234567;
		final long numSubjects = 1000;
		final int maxYZ = 200;
		final long seed = 27;

		Random rnd = new Random(seed);

		List<TripleID> list = new ArrayList<>();

		for (int s = 1; s <= numSubjects; s++) {
			int numYZ = rnd.nextInt(maxYZ) + 1;
			for (int i = 0; i < numYZ; i++) {
				list.add(new TripleID(s, rnd.nextInt(maxY) + 1, rnd.nextInt(maxZ) + 1));
			}
		}

		list.sort(TripleID::compareTo);

		BitTripleWriter writer = new BitTripleWriter(os, BitUtil.log2(maxY), BitUtil.log2(maxZ));

		for (TripleID tid : list) {
			writer.appendTriple(tid);
		}

		writer.end();

		byte[] byteArray = os.toByteArray();
		ByteArrayInputStream is = new ByteArrayInputStream(byteArray);

		BitTripleReader reader = new BitTripleReader(is);
		Iterator<TripleID> it = list.iterator();
		while (reader.hasNext()) {
			assertTrue(it.hasNext());
			TripleID next = reader.next();
			TripleID nextex = it.next();
			assertEquals(nextex, next);
		}
		assertFalse(it.hasNext());
	}

	private void testCompress(CompressionType type, byte[] data) {
		int originalLen = data.length;
		int compLen = type.debugCompress(data).length;
		System.out.println(type.name() + " " + compLen + " / " + originalLen + " "
				+ ((100L * (originalLen - compLen)) / originalLen) + "%");
	}

	@Test
	@Ignore("hand")
	public void handTest() throws IOException {
		ByteArrayOutputStream os = new ByteArrayOutputStream();

		final int maxY = 12345;
		final int maxZ = 1234567;
		final long numSubjects = 100000;
		final int maxYZ = 200;
		final long seed = 27;

		Random rnd = new Random(seed);

		List<TripleID> list = new ArrayList<>();

		for (int s = 1; s <= numSubjects; s++) {
			int numYZ = rnd.nextInt(maxYZ) + 1;
			for (int i = 0; i < numYZ; i++) {
				list.add(new TripleID(s, rnd.nextInt(maxY) + 1, rnd.nextInt(maxZ) + 1));
			}
		}

		list.sort(TripleID::compareTo);

		BitTripleWriter writer = new BitTripleWriter(os, BitUtil.log2(maxY), BitUtil.log2(maxZ));

		for (TripleID tid : list) {
			writer.appendTriple(tid);
		}

		writer.end();

		byte[] byteArray = os.toByteArray();

		System.out.println("data " + byteArray.length);

		testCompress(CompressionType.LZ4, byteArray);
		testCompress(CompressionType.LZ4B, byteArray);
		testCompress(CompressionType.LZMA, byteArray);
		testCompress(CompressionType.BZIP, byteArray);
		testCompress(CompressionType.GZIP, byteArray);
		testCompress(CompressionType.XZ, byteArray);
		testCompress(CompressionType.NONE, byteArray);

		/*
		 * nice: data 46492764 LZ4 46492823 / 46492764 0% LZ4B 46507695 /
		 * 46492764 0% LZMA 46989726 / 46492764 -1% BZIP 46719216 / 46492764 0%
		 * GZIP 46506967 / 46492764 0% XZ 46494980 / 46492764 0% NONE 46492764 /
		 * 46492764 0%
		 */
	}

}
