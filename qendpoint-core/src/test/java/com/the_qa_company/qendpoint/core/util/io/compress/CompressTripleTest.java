package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.triples.IndexedNode;
import com.the_qa_company.qendpoint.core.triples.IndexedTriple;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import org.junit.Assert;
import org.junit.Test;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.util.concurrent.ExceptionThread;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class CompressTripleTest {
	@Test
	public void writeReadTest() throws InterruptedException, IOException {
		try (PipedOutputStream out = new PipedOutputStream(); PipedInputStream in = new PipedInputStream()) {
			out.connect(in);
			List<IndexedTriple> triples = Arrays.asList(
					new IndexedTriple(new IndexedNode("", 1), new IndexedNode("", 9), new IndexedNode("", 11)),
					new IndexedTriple(new IndexedNode("", 1), new IndexedNode("", 9), new IndexedNode("", 11)),
					new IndexedTriple(new IndexedNode("", 3), new IndexedNode("", 10), new IndexedNode("", 11)),
					new IndexedTriple(new IndexedNode("", 2), new IndexedNode("", 12), new IndexedNode("", 15)),
					new IndexedTriple(new IndexedNode("", 2), new IndexedNode("", 12), new IndexedNode("", 15)),
					new IndexedTriple(new IndexedNode("", 6), new IndexedNode("", 14), new IndexedNode("", 13)));
			List<IndexedTriple> noDupeTriples = Arrays.asList(
					new IndexedTriple(new IndexedNode("", 1), new IndexedNode("", 9), new IndexedNode("", 11)),
					new IndexedTriple(new IndexedNode("", 3), new IndexedNode("", 10), new IndexedNode("", 11)),
					new IndexedTriple(new IndexedNode("", 2), new IndexedNode("", 12), new IndexedNode("", 15)),
					new IndexedTriple(new IndexedNode("", 6), new IndexedNode("", 14), new IndexedNode("", 13)));
			new ExceptionThread(() -> {
				CompressTripleReader reader = new CompressTripleReader(in);
				try {
					for (IndexedTriple exceptedIndex : noDupeTriples) {
						Assert.assertTrue(reader.hasNext());
						TripleID actual = reader.next();
						TripleID excepted = new TripleID(exceptedIndex.getSubject().getIndex(),
								exceptedIndex.getPredicate().getIndex(), exceptedIndex.getObject().getIndex());
						Assert.assertEquals(excepted, actual);
					}
					Assert.assertFalse(reader.hasNext());
					Assert.assertEquals(34, in.read());
					Assert.assertEquals(12, in.read());
					Assert.assertEquals(27, in.read());
				} finally {
					in.close();
				}
			}, "ReadTest").attach(new ExceptionThread(() -> {
				CompressTripleWriter writer = new CompressTripleWriter(out, false);
				try {
					for (IndexedTriple triple : triples) {
						writer.appendTriple(triple);
					}
					writer.writeCRC();
					// raw data to check if we didn't read too/not enough data
					out.write(34);
					out.write(12);
					out.write(27);
				} finally {
					out.close();
				}
			}, "WriteTest")).startAll().joinAndCrashIfRequired();
		}
	}

	@Test
	public void writeReadTripleIDTest() throws InterruptedException, IOException {
		try (PipedOutputStream out = new PipedOutputStream(); PipedInputStream in = new PipedInputStream()) {
			out.connect(in);
			List<TripleID> triples = Arrays.asList(new TripleID(1, 9, 11), new TripleID(1, 9, 11),
					new TripleID(3, 10, 11), new TripleID(2, 12, 15), new TripleID(2, 12, 15), new TripleID(6, 14, 13));
			List<TripleID> noDupeTriples = Arrays.asList(new TripleID(1, 9, 11), new TripleID(3, 10, 11),
					new TripleID(2, 12, 15), new TripleID(6, 14, 13));
			new ExceptionThread(() -> {
				CompressTripleReader reader = new CompressTripleReader(in);
				try {
					for (TripleID excepted : noDupeTriples) {
						Assert.assertTrue(reader.hasNext());
						TripleID actual = reader.next();
						Assert.assertEquals(excepted, actual);
					}
					Assert.assertFalse(reader.hasNext());
					Assert.assertEquals(34, in.read());
					Assert.assertEquals(12, in.read());
					Assert.assertEquals(27, in.read());
				} finally {
					in.close();
				}
			}, "ReadTest").attach(new ExceptionThread(() -> {
				CompressTripleWriter writer = new CompressTripleWriter(out, false);
				try {
					for (TripleID triple : triples) {
						writer.appendTriple(triple);
					}
					writer.writeCRC();
					// raw data to check if we didn't read too/not enough data
					out.write(34);
					out.write(12);
					out.write(27);
				} finally {
					out.close();
				}
			}, "WriteTest")).startAll().joinAndCrashIfRequired();
		}
	}

	@Test
	public void writeReadMergeTest() {
		List<TripleID> triples1 = Arrays.asList(new TripleID(2, 2, 2), new TripleID(4, 4, 4), new TripleID(5, 5, 5));
		List<TripleID> triples2 = Arrays.asList(new TripleID(1, 1, 1), new TripleID(3, 3, 3), new TripleID(6, 6, 6));
		List<TripleID> triplesFinal = Arrays.asList(new TripleID(1, 1, 1), new TripleID(2, 2, 2), new TripleID(3, 3, 3),
				new TripleID(4, 4, 4), new TripleID(5, 5, 5), new TripleID(6, 6, 6));
		Iterator<TripleID> actual = new CompressTripleMergeIterator(ExceptionIterator.of(triples1.iterator()),
				ExceptionIterator.of(triples2.iterator()), TripleComponentOrder.SPO).asIterator();
		Iterator<TripleID> expected = triplesFinal.iterator();

		expected.forEachRemaining(tid -> {
			Assert.assertTrue(actual.hasNext());
			Assert.assertEquals(tid, actual.next());
		});
		Assert.assertFalse(actual.hasNext());

	}

}
