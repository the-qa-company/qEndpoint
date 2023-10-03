package com.the_qa_company.qendpoint.core.dictionary.impl;

import com.the_qa_company.qendpoint.core.dictionary.DictionaryPrivate;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionary;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.OneReadDictionarySection;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.CompressionResult;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.triples.IndexedNode;
import com.the_qa_company.qendpoint.core.triples.TempTriples;
import com.the_qa_company.qendpoint.core.util.io.compress.CompressUtil;
import com.the_qa_company.qendpoint.core.utils.DebugOrderNodeIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.MapIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.NotificationExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.PipedCopyIterator;
import com.the_qa_company.qendpoint.core.util.concurrent.ExceptionThread;
import com.the_qa_company.qendpoint.core.util.string.CharSequenceComparator;
import com.the_qa_company.qendpoint.core.util.string.CompactString;

import java.io.IOException;
import java.util.Comparator;
import java.util.function.Consumer;

/**
 * Version of temp dictionary create the four sections from the SPO compressed
 * sections result, should be loaded in a async way with
 * {@link DictionaryPrivate#loadAsync(TempDictionary, ProgressListener)}
 *
 * @author Antoine Willerval
 */
public class CompressFourSectionDictionary implements TempDictionary {
	private final ExceptionThread cfsdThread;
	private final TempDictionarySection subject;
	private final TempDictionarySection predicate;
	private final TempDictionarySection object;
	private final TempDictionarySection shared;
	private final TempDictionarySection graph;

	private static void sendPiped(IndexedNode node, long index, PipedCopyIterator<CharSequence> pipe,
			CompressUtil.DuplicatedIterator it, NodeConsumerMethod method) {
		it.setLastHeader(index);
		method.consume(node.getIndex(), index);
		pipe.addElement(new CompactString(node.getNode()));
	}

	public CompressFourSectionDictionary(CompressionResult compressionResult, NodeConsumer nodeConsumer,
			ProgressListener listener, boolean debugOrder, boolean quad) {
		long splits = Math.max(20, compressionResult.getTripleCount() / 10_000);
		Consumer<IndexedNode> debugOrderCheckerS = DebugOrderNodeIterator.of(debugOrder, "Subject");
		Consumer<IndexedNode> debugOrderCheckerO = DebugOrderNodeIterator.of(debugOrder, "Object");
		// send duplicate to the consumer while reading the nodes
		CompressUtil.DuplicatedIterator sortedSubject = CompressUtil.asNoDupeCharSequenceIterator(
				new NotificationExceptionIterator<>(compressionResult.getSubjects(), compressionResult.getTripleCount(),
						splits, "Subject section filling", listener),
				(originalIndex, duplicatedIndex, lastHeader) -> nodeConsumer.onSubject(duplicatedIndex, lastHeader));
		CompressUtil.DuplicatedIterator sortedPredicate = CompressUtil.asNoDupeCharSequenceIterator(
				new NotificationExceptionIterator<>(compressionResult.getPredicates(),
						compressionResult.getTripleCount(), splits, "Predicate section filling", listener),
				(originalIndex, duplicatedIndex, lastHeader) -> nodeConsumer.onPredicate(duplicatedIndex, lastHeader));
		CompressUtil.DuplicatedIterator sortedObject = CompressUtil.asNoDupeCharSequenceIterator(
				new NotificationExceptionIterator<>(compressionResult.getObjects(), compressionResult.getTripleCount(),
						splits, "Object section filling", listener),
				(originalIndex, duplicatedIndex, lastHeader) -> nodeConsumer.onObject(duplicatedIndex, lastHeader));
		CompressUtil.DuplicatedIterator sortedGraph;
		if (quad) {
			sortedGraph = CompressUtil.asNoDupeCharSequenceIterator(
					new NotificationExceptionIterator<>(compressionResult.getGraph(),
							compressionResult.getTripleCount(), splits, "Graph section filling", listener),
					(originalIndex, duplicatedIndex, lastHeader) -> nodeConsumer.onGraph(duplicatedIndex, lastHeader));
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
							sendPiped(newSubject, CompressUtil.getHeaderId(subjectId++), subject, sortedSubject,
									nodeConsumer::onSubject);
							if (!sortedSubject.hasNext()) {
								// no more subjects, send the current object and
								// break the shared loop
								sendPiped(newObject, CompressUtil.getHeaderId(objectId++), object, sortedObject,
										nodeConsumer::onObject);
								break sharedLoop;
							}
							newSubject = sortedSubject.next();
							debugOrderCheckerS.accept(newSubject);
						} else {
							sendPiped(newObject, CompressUtil.getHeaderId(objectId++), object, sortedObject,
									nodeConsumer::onObject);
							if (!sortedObject.hasNext()) {
								// no more objects, send the current subject and
								// break the shared loop
								sendPiped(newSubject, CompressUtil.getHeaderId(subjectId++), subject, sortedSubject,
										nodeConsumer::onSubject);
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
					nodeConsumer.onSubject(newSubject.getIndex(), shid);
					nodeConsumer.onObject(newObject.getIndex(), shid);
					shared.addElement(new CompactString(newSubject.getNode()));
				}
				// at least one iterator is empty, closing the shared pipe
				shared.closePipe();
				// do we have subjects?
				while (sortedSubject.hasNext()) {
					IndexedNode next = sortedSubject.next();
					debugOrderCheckerS.accept(next);
					sendPiped(next, CompressUtil.getHeaderId(subjectId++), subject, sortedSubject,
							nodeConsumer::onSubject);
				}
				subject.closePipe();
				// do we have objects?
				while (sortedObject.hasNext()) {
					IndexedNode next = sortedObject.next();
					debugOrderCheckerO.accept(next);
					sendPiped(next, CompressUtil.getHeaderId(objectId++), object, sortedObject, nodeConsumer::onObject);
				}
				object.closePipe();
			} catch (Throwable t) {
				object.closePipe(t);
				subject.closePipe(t);
				shared.closePipe(t);
				throw t;
			}
		}, "CFSDPipeBuilder").startAll();

		// send to the consumer the element while parsing them
		this.subject = new OneReadDictionarySection(subject, subjects);
		this.predicate = new OneReadDictionarySection(new MapIterator<>(sortedPredicate, (node, index) -> {
			long header = CompressUtil.getHeaderId(index + 1);
			sortedPredicate.setLastHeader(header);
			nodeConsumer.onPredicate(node.getIndex(), header);
			// force duplication because it's not made in a pipe like with the
			// others
			return new CompactString(node.getNode());
		}), predicates);
		this.object = new OneReadDictionarySection(object, objects);
		this.shared = new OneReadDictionarySection(shared, shareds);
		if (quad) {
			this.graph = new OneReadDictionarySection(new MapIterator<>(sortedGraph, (node, index) -> {
				long header = CompressUtil.getHeaderId(index + 1);
				sortedGraph.setLastHeader(header);
				nodeConsumer.onGraph(node.getIndex(), header);
				// force duplication because it's not made in a pipe like with
				// the
				// others
				return new CompactString(node.getNode());
			}), graphs);
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

		void onPredicate(long preMapId, long newMapId);

		void onObject(long preMapId, long newMapId);

		void onGraph(long preMapId, long newMapId);
	}

	private interface NodeConsumerMethod {
		void consume(long id, long header);
	}
}
