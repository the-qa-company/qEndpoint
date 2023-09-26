package com.the_qa_company.qendpoint.core.dictionary.impl;

import com.the_qa_company.qendpoint.core.triples.IndexedNode;
import com.the_qa_company.qendpoint.core.util.io.compress.CompressTest;
import org.junit.Assert;
import org.junit.Test;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.CompressionResult;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.MapIterator;
import com.the_qa_company.qendpoint.core.util.concurrent.ExceptionThread;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.ByteStringUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class CompressFourSectionDictionaryTest {
	@Test
	public void compressDictTest() throws Exception {
		TestCompressionResult result = new TestCompressionResult(
				new CharSequence[] { "2222", "4444", "5555", "7777", "9999", "9999" },
				new CharSequence[] { "1111", "1111", "2222", "3333", "3333", "4444" },
				new CharSequence[] { "1111", "3333", "3333", "4444", "6666", "7777", "8888" });
		List<CharSequence> exceptedSubjects = Arrays.asList("2222", "5555", "9999");
		List<CharSequence> exceptedPredicates = Arrays.asList("1111", "2222", "3333", "4444");
		List<CharSequence> exceptedObjects = Arrays.asList("1111", "3333", "6666", "8888");
		List<CharSequence> exceptedShared = Arrays.asList("4444", "7777");
		CompressFourSectionDictionary dictionary = new CompressFourSectionDictionary(result, new FakeNodeConsumer(),
				(p, m) -> {}, true, false);
		Iterator<? extends CharSequence> su = dictionary.getSubjects().getSortedEntries();
		Iterator<? extends CharSequence> pr = dictionary.getPredicates().getSortedEntries();
		Iterator<? extends CharSequence> ob = dictionary.getObjects().getSortedEntries();
		Iterator<? extends CharSequence> sh = dictionary.getShared().getSortedEntries();
		ExceptionThread subjectReader = new ExceptionThread(() -> {
			for (CharSequence e : exceptedSubjects) {
				Assert.assertTrue(su.hasNext());
				CharSequence a = su.next();
				Thread.sleep(40);
				CompressTest.assertCharSequenceEquals("Subject", e, a);
			}
		}, "compressDictTestS");
		ExceptionThread predicateReader = new ExceptionThread(() -> {
			for (CharSequence e : exceptedPredicates) {
				Assert.assertTrue(pr.hasNext());
				CharSequence a = pr.next();
				Thread.sleep(40);
				CompressTest.assertCharSequenceEquals("Predicate", e, a);
			}
		}, "compressDictTestP");
		ExceptionThread objectReader = new ExceptionThread(() -> {
			for (CharSequence e : exceptedObjects) {
				Assert.assertTrue(ob.hasNext());
				CharSequence a = ob.next();
				Thread.sleep(40);
				CompressTest.assertCharSequenceEquals("Object", e, a);
			}
		}, "compressDictTestO");
		ExceptionThread sharedReader = new ExceptionThread(() -> {
			for (CharSequence e : exceptedShared) {
				Assert.assertTrue(sh.hasNext());
				CharSequence a = sh.next();
				Thread.sleep(40);
				CompressTest.assertCharSequenceEquals("Shared", e, a);
			}
		}, "compressDictTestSh");

		sharedReader.attach(predicateReader, objectReader, subjectReader).startAll().joinAndCrashIfRequired();
	}

	static class TestCompressionResult implements CompressionResult {
		private final CharSequence[] subjects;
		private final CharSequence[] predicates;
		private final CharSequence[] objects;
		private final CharSequence[] graph;
		// used to create fake id to avoid duplicate assert error
		private int sid, pid, oid, gid;

		private final long size;

		public TestCompressionResult(CharSequence[] subjects, CharSequence[] predicates, CharSequence[] objects) {
			this(subjects, predicates, objects, null);
		}

		public TestCompressionResult(CharSequence[] subjects, CharSequence[] predicates, CharSequence[] objects,
				CharSequence[] graph) {
			this.subjects = subjects;
			this.predicates = predicates;
			this.objects = objects;
			this.graph = graph;

			size = Arrays.stream(subjects).mapToLong(s -> s.toString().getBytes(ByteStringUtil.STRING_ENCODING).length)
					.sum()
					+ Arrays.stream(predicates)
							.mapToLong(s -> s.toString().getBytes(ByteStringUtil.STRING_ENCODING).length).sum()
					+ Arrays.stream(objects)
							.mapToLong(s -> s.toString().getBytes(ByteStringUtil.STRING_ENCODING).length).sum()
					+ (graph == null ? 0
							: Arrays.stream(graph)
									.mapToLong(s -> s.toString().getBytes(ByteStringUtil.STRING_ENCODING).length)
									.sum());
		}

		@Override
		public long getTripleCount() {
			return Math.max(subjects.length, Math.max(predicates.length, objects.length));
		}

		@Override
		public boolean supportsGraph() {
			return graph != null;
		}

		@Override
		public ExceptionIterator<IndexedNode, IOException> getSubjects() {
			return ExceptionIterator.of(new MapIterator<>(Arrays.asList(subjects).iterator(),
					s -> new IndexedNode(ByteString.of(s), sid++)));
		}

		@Override
		public ExceptionIterator<IndexedNode, IOException> getPredicates() {
			return ExceptionIterator.of(new MapIterator<>(Arrays.asList(predicates).iterator(),
					s -> new IndexedNode(ByteString.of(s), pid++)));
		}

		@Override
		public ExceptionIterator<IndexedNode, IOException> getObjects() {
			return ExceptionIterator.of(new MapIterator<>(Arrays.asList(objects).iterator(),
					s -> new IndexedNode(ByteString.of(s), oid++)));
		}

		@Override
		public ExceptionIterator<IndexedNode, IOException> getGraph() {
			return ExceptionIterator.of(
					new MapIterator<>(Arrays.asList(graph).iterator(), g -> new IndexedNode(ByteString.of(g), gid++)));
		}

		@Override
		public long getSubjectsCount() {
			return subjects.length;
		}

		@Override
		public long getPredicatesCount() {
			return predicates.length;
		}

		@Override
		public long getObjectsCount() {
			return objects.length;
		}

		@Override
		public long getGraphCount() {
			return graph.length;
		}

		@Override
		public long getSharedCount() {
			return Math.min(subjects.length, objects.length);
		}

		@Override
		public void delete() {
		}

		@Override
		public long getRawSize() {
			return size;
		}

		@Override
		public void close() {
		}
	}

	static class FakeNodeConsumer implements CompressFourSectionDictionary.NodeConsumer {
		@Override
		public void onSubject(long preMapId, long newMapId) {
		}

		@Override
		public void onPredicate(long preMapId, long newMapId) {
		}

		@Override
		public void onObject(long preMapId, long newMapId) {
		}

		@Override
		public void onGraph(long preMapId, long newMapId) {
		}
	}
}
