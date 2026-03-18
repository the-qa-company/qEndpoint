package com.the_qa_company.qendpoint.core.dictionary.impl;

import com.the_qa_company.qendpoint.core.dictionary.DictionaryPrivate;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionary;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.OneReadDictionarySection;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.CompressionResult;
import com.the_qa_company.qendpoint.core.iterator.utils.MapIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.NotificationExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.PipedCopyIterator;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.triples.IndexedNode;
import com.the_qa_company.qendpoint.core.triples.TempTriples;
import com.the_qa_company.qendpoint.core.util.concurrent.ExceptionThread;
import com.the_qa_company.qendpoint.core.util.io.compress.CompressUtil;
import com.the_qa_company.qendpoint.core.util.string.CharSequenceComparator;
import com.the_qa_company.qendpoint.core.util.string.CompactString;
import com.the_qa_company.qendpoint.core.utils.DebugOrderNodeIterator;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.function.Consumer;

/**
 * Version of temp dictionary create the four sections from the SPO compressed
 * sections result, should be loaded in a async way with
 * {@link DictionaryPrivate#loadAsync(TempDictionary, ProgressListener)}
 *
 * @author Antoine Willerval
 */
public class CompressFourSectionDictionary implements TempDictionary {
	private static final int PIPE_BULK_BUFFER_SIZE = PipedCopyIterator.BATCH_SIZE;

	private final ExceptionThread cfsdThread;
	private final TempDictionarySection subject;
	private final TempDictionarySection predicate;
	private final TempDictionarySection object;
	private final TempDictionarySection shared;
	private final TempDictionarySection graph;

	private static void sendPiped(long[] ids, long[] headers, CharSequence[] nodes, int length,
			PipedCopyIterator<CharSequence> pipe, NodeConsumerBulkMethod method) {
		method.consume(ids, headers, 0, length);
		for (int i = 0; i < length; i++) {
			pipe.addElement(nodes[i]);
		}
	}

	private static final class BulkNodeBuffer {
		private final long[] ids = new long[PIPE_BULK_BUFFER_SIZE];
		private final long[] headers = new long[PIPE_BULK_BUFFER_SIZE];
		private final CharSequence[] nodes = new CharSequence[PIPE_BULK_BUFFER_SIZE];
		private int size;
		private final PipedCopyIterator<CharSequence> pipe;
		private final CompressUtil.DuplicatedIterator it;
		private final NodeConsumerBulkMethod method;

		private BulkNodeBuffer(PipedCopyIterator<CharSequence> pipe, CompressUtil.DuplicatedIterator it,
				NodeConsumerBulkMethod method) {
			this.pipe = pipe;
			this.it = it;
			this.method = method;
		}

		private void add(IndexedNode node, long header) {
			it.setLastHeader(header);
			ids[size] = node.getIndex();
			headers[size] = header;
			nodes[size] = new CompactString(node.getNode());
			size++;
			if (size == PIPE_BULK_BUFFER_SIZE) {
				flush();
			}
		}

		private void flush() {
			if (size == 0) {
				return;
			}
			sendPiped(ids, headers, nodes, size, pipe, method);
			size = 0;
		}
	}

	private static final class BulkMappingBuffer {
		private final long[] ids = new long[PIPE_BULK_BUFFER_SIZE];
		private final long[] headers = new long[PIPE_BULK_BUFFER_SIZE];
		private int size;
		private final NodeConsumerBulkMethod method;

		private BulkMappingBuffer(NodeConsumerBulkMethod method) {
			this.method = method;
		}

		private void add(long id, long header) {
			ids[size] = id;
			headers[size] = header;
			size++;
			if (size == PIPE_BULK_BUFFER_SIZE) {
				flush();
			}
		}

		private void flush() {
			if (size == 0) {
				return;
			}
			method.consume(ids, headers, 0, size);
			size = 0;
		}
	}

	private static <T> Iterator<T> flushOnFinish(Iterator<T> iterator, Runnable onFinish) {
		return new Iterator<>() {
			private boolean finished;

			@Override
			public boolean hasNext() {
				boolean hasNext = iterator.hasNext();
				if (!hasNext) {
					finish();
				}
				return hasNext;
			}

			@Override
			public T next() {
				T next = iterator.next();
				if (!iterator.hasNext()) {
					finish();
				}
				return next;
			}

			private void finish() {
				if (finished) {
					return;
				}
				finished = true;
				onFinish.run();
			}
		};
	}

	public CompressFourSectionDictionary(CompressionResult compressionResult, NodeConsumer nodeConsumer,
			ProgressListener listener, boolean debugOrder, boolean quad) {
		long splits = Math.max(20, compressionResult.getTripleCount() / 10_000);
		Consumer<IndexedNode> debugOrderCheckerS = DebugOrderNodeIterator.of(debugOrder, "Subject");
		Consumer<IndexedNode> debugOrderCheckerO = DebugOrderNodeIterator.of(debugOrder, "Object");
		NodeConsumerBulkMethod subjectBulk = nodeConsumer::onSubject;
		NodeConsumerBulkMethod predicateBulk = nodeConsumer::onPredicate;
		NodeConsumerBulkMethod objectBulk = nodeConsumer::onObject;
		NodeConsumerBulkMethod graphBulk = nodeConsumer::onGraph;
		BulkMappingBuffer subjectDuplicateBuffer = new BulkMappingBuffer(subjectBulk);
		BulkMappingBuffer predicateDuplicateBuffer = new BulkMappingBuffer(predicateBulk);
		BulkMappingBuffer objectDuplicateBuffer = new BulkMappingBuffer(objectBulk);
		BulkMappingBuffer graphDuplicateBuffer = quad ? new BulkMappingBuffer(graphBulk) : null;
		// send duplicate to the consumer while reading the nodes
		CompressUtil.DuplicatedIterator sortedSubject = CompressUtil.asNoDupeCharSequenceIterator(
				new NotificationExceptionIterator<>(compressionResult.getSubjects(), compressionResult.getTripleCount(),
						splits, "Subject section filling", listener),
				(originalIndex, duplicatedIndex, lastHeader) -> subjectDuplicateBuffer.add(duplicatedIndex,
						lastHeader));

		CompressUtil.DuplicatedIterator sortedPredicate = CompressUtil.asNoDupeCharSequenceIterator(
				new NotificationExceptionIterator<>(compressionResult.getPredicates(),
						compressionResult.getTripleCount(), splits, "Predicate section filling", listener),
				(originalIndex, duplicatedIndex, lastHeader) -> predicateDuplicateBuffer.add(duplicatedIndex,
						lastHeader));

		CompressUtil.DuplicatedIterator sortedObject = CompressUtil.asNoDupeCharSequenceIterator(
				new NotificationExceptionIterator<>(compressionResult.getObjects(), compressionResult.getTripleCount(),
						splits, "Object section filling", listener),
				(originalIndex, duplicatedIndex, lastHeader) -> objectDuplicateBuffer.add(duplicatedIndex, lastHeader));

		CompressUtil.DuplicatedIterator sortedGraph;
		if (quad) {
			sortedGraph = CompressUtil.asNoDupeCharSequenceIterator(
					new NotificationExceptionIterator<>(compressionResult.getGraph(),
							compressionResult.getTripleCount(), splits, "Graph section filling", listener),
					(originalIndex, duplicatedIndex, lastHeader) -> graphDuplicateBuffer.add(duplicatedIndex,
							lastHeader));
		} else {
			sortedGraph = null;
		}
		long subjects = compressionResult.getSubjectsCount();
		long predicates = compressionResult.getPredicatesCount();
		long objects = compressionResult.getObjectsCount();
		long shareds = compressionResult.getSharedCount();
		long graphs = quad ? compressionResult.getGraphCount() : 0;

		// iterator to pipe to the s p o sh
		PipedCopyIterator<CharSequence> subject = new PipedCopyIterator<>();
		PipedCopyIterator<CharSequence> object = new PipedCopyIterator<>();
		PipedCopyIterator<CharSequence> shared = new PipedCopyIterator<>();
		BulkNodeBuffer subjectPipeBuffer = new BulkNodeBuffer(subject, sortedSubject, subjectBulk);
		BulkNodeBuffer objectPipeBuffer = new BulkNodeBuffer(object, sortedObject, objectBulk);
		Comparator<CharSequence> comparator = CharSequenceComparator.getInstance();
		cfsdThread = new ExceptionThread(() -> {
			try {
				long sharedId = 1;
				long subjectId = 1;
				long objectId = 1;
				sharedLoop:
				while (sortedObject.hasNext() && sortedSubject.hasNext()) {
					// last was a shared node
					IndexedNode newSubject = sortedSubject.next();
					IndexedNode newObject = sortedObject.next();
					debugOrderCheckerS.accept(newSubject);
					debugOrderCheckerO.accept(newObject);
					int comp = comparator.compare(newSubject.getNode(), newObject.getNode());
					while (comp != 0) {
						if (comp < 0) {
							subjectPipeBuffer.add(newSubject, CompressUtil.getHeaderId(subjectId++));
							if (!sortedSubject.hasNext()) {
								// no more subjects, send the current object and
								// break the shared loop
								objectPipeBuffer.add(newObject, CompressUtil.getHeaderId(objectId++));
								break sharedLoop;
							}
							newSubject = sortedSubject.next();
							debugOrderCheckerS.accept(newSubject);
						} else {
							objectPipeBuffer.add(newObject, CompressUtil.getHeaderId(objectId++));
							if (!sortedObject.hasNext()) {
								// no more objects, send the current subject and
								// break the shared loop
								subjectPipeBuffer.add(newSubject, CompressUtil.getHeaderId(subjectId++));
								break sharedLoop;
							}
							newObject = sortedObject.next();
							debugOrderCheckerO.accept(newObject);
						}
						comp = comparator.compare(newSubject.getNode(), newObject.getNode());
					}
					// shared element
					long shid = CompressUtil.asShared(sharedId++);
					sortedSubject.setLastHeader(shid);
					sortedObject.setLastHeader(shid);
					subjectDuplicateBuffer.add(newSubject.getIndex(), shid);
					objectDuplicateBuffer.add(newObject.getIndex(), shid);
					shared.addElement(new CompactString(newSubject.getNode()));
				}
				// at least one iterator is empty, closing the shared pipe
				subjectPipeBuffer.flush();
				objectPipeBuffer.flush();
				subjectDuplicateBuffer.flush();
				objectDuplicateBuffer.flush();
				shared.closePipe();
				// do we have subjects?
				while (sortedSubject.hasNext()) {
					IndexedNode next = sortedSubject.next();
					debugOrderCheckerS.accept(next);
					subjectPipeBuffer.add(next, CompressUtil.getHeaderId(subjectId++));
				}
				subjectPipeBuffer.flush();
				subjectDuplicateBuffer.flush();
				subject.closePipe();
				// do we have objects?
				while (sortedObject.hasNext()) {
					IndexedNode next = sortedObject.next();
					debugOrderCheckerO.accept(next);
					objectPipeBuffer.add(next, CompressUtil.getHeaderId(objectId++));
				}
				objectPipeBuffer.flush();
				objectDuplicateBuffer.flush();
				object.closePipe();

			} catch (Throwable t) {
				try {
					subjectPipeBuffer.flush();
					objectPipeBuffer.flush();
					subjectDuplicateBuffer.flush();
					objectDuplicateBuffer.flush();
				} catch (Throwable ignore) {
					// ignore (already failing)
				}
				object.closePipe(t);
				subject.closePipe(t);
				shared.closePipe(t);
				throw t;
			}
		}, "CFSDPipeBuilder").startAll();

		// send to the consumer the element while parsing them
		this.subject = new OneReadDictionarySection(subject, subjects);
		this.predicate = new OneReadDictionarySection(
				flushOnFinish(new MapIterator<>(sortedPredicate, (node, index) -> {
					long header = CompressUtil.getHeaderId(index + 1);
					sortedPredicate.setLastHeader(header);
					predicateDuplicateBuffer.add(node.getIndex(), header);
					// force duplication because it's not made in a pipe like
					// with the
					// others
					return new CompactString(node.getNode());
				}), predicateDuplicateBuffer::flush), predicates);
		this.object = new OneReadDictionarySection(object, objects);
		this.shared = new OneReadDictionarySection(shared, shareds);
		if (quad) {
			this.graph = new OneReadDictionarySection(flushOnFinish(new MapIterator<>(sortedGraph, (node, index) -> {
				long header = CompressUtil.getHeaderId(index + 1);
				sortedGraph.setLastHeader(header);
				graphDuplicateBuffer.add(node.getIndex(), header);
				// force duplication because it's not made in a pipe like with
				// the
				// others
				return new CompactString(node.getNode());
			}), graphDuplicateBuffer::flush), graphs);
		} else {
			this.graph = null;
		}
	}

	@Override
	public TempDictionarySection getSubjects() {
		return subject;
	}

	@Override
	public TempDictionarySection getPredicates() {
		return predicate;
	}

	@Override
	public TempDictionarySection getObjects() {
		return object;
	}

	@Override
	public TempDictionarySection getShared() {
		return shared;
	}

	@Override
	public TempDictionarySection getGraphs() {
		if (supportGraphs()) {
			return graph;
		}
		throw new IllegalArgumentException("This dictionary doesn't support graph!");
	}

	@Override
	public boolean supportGraphs() {
		return graph != null;
	}

	@Override
	public void startProcessing() {
	}

	@Override
	public void endProcessing() {
	}

	@Override
	public long insert(CharSequence str, TripleComponentRole position) {
		throw new NotImplementedException();
	}

	@Override
	public void reorganize() {
		// already organized
	}

	@Override
	public void reorganize(TempTriples triples) {
		// already organized
	}

	@Override
	public boolean isOrganized() {
		return true;
	}

	@Override
	public void clear() {
	}

	@Override
	public long stringToId(CharSequence subject, TripleComponentRole role) {
		throw new NotImplementedException();
	}

	@Override
	public void close() throws IOException {
		try {
			cfsdThread.interrupt();
			cfsdThread.joinAndCrashIfRequired();
		} catch (InterruptedException e) {
			// normal
		}
	}

	public interface NodeConsumer {
		void onSubject(long preMapId, long newMapId);

		default void onSubject(long[] preMapIds, long[] newMapIds, int offset, int length) {
			for (int i = offset; i < offset + length; i++) {
				onSubject(preMapIds[i], newMapIds[i]);
			}
		}

		void onPredicate(long preMapId, long newMapId);

		default void onPredicate(long[] preMapIds, long[] newMapIds, int offset, int length) {
			for (int i = offset; i < offset + length; i++) {
				onPredicate(preMapIds[i], newMapIds[i]);
			}
		}

		void onObject(long preMapId, long newMapId);

		default void onObject(long[] preMapIds, long[] newMapIds, int offset, int length) {
			for (int i = offset; i < offset + length; i++) {
				onObject(preMapIds[i], newMapIds[i]);
			}
		}

		void onGraph(long preMapId, long newMapId);

		default void onGraph(long[] preMapIds, long[] newMapIds, int offset, int length) {
			for (int i = offset; i < offset + length; i++) {
				onGraph(preMapIds[i], newMapIds[i]);
			}
		}
	}

	@FunctionalInterface
	private interface NodeConsumerBulkMethod {
		void consume(long[] id, long[] header, int offset, int length);
	}
}
