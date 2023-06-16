package com.the_qa_company.qendpoint.core.dictionary.impl;

import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.WriteDictionarySection;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.header.Header;
import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.ControlInfo;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.iterator.utils.PeekIteratorImpl;
import com.the_qa_company.qendpoint.core.iterator.utils.PipedCopyIterator;
import com.the_qa_company.qendpoint.core.util.concurrent.ExceptionThread;
import com.the_qa_company.qendpoint.core.util.io.CountInputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.listener.IntermediateListener;
import com.the_qa_company.qendpoint.core.util.listener.ListenerUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Version of mutli-section dictionary with {@link WriteDictionarySection}
 *
 * @author Antoine Willerval
 */
public class WriteMultipleSectionDictionary extends MultipleBaseDictionary {
	private final Path filename;
	private final int bufferSize;

	public WriteMultipleSectionDictionary(HDTOptions spec, Path filename, int bufferSize) {
		super(spec);
		this.filename = filename;
		this.bufferSize = bufferSize;
		String name = filename.getFileName().toString();
		subjects = new WriteDictionarySection(spec, filename.resolveSibling(name + "SU"), bufferSize);
		predicates = new WriteDictionarySection(spec, filename.resolveSibling(name + "PR"), bufferSize);
		objects = new TreeMap<>();
		shared = new WriteDictionarySection(spec, filename.resolveSibling(name + "SH"), bufferSize);
	}

	public WriteMultipleSectionDictionary(HDTOptions spec, DictionarySectionPrivate subjects,
			DictionarySectionPrivate predicates, DictionarySectionPrivate shared,
			TreeMap<ByteString, DictionarySectionPrivate> objects) {
		super(spec);
		// useless
		this.filename = null;
		this.bufferSize = 0;

		// write sections
		this.subjects = subjects;
		this.predicates = predicates;
		this.objects = objects;
		this.shared = shared;
	}

	@Override
	public long getNAllObjects() {
		return objects.values().stream().mapToLong(DictionarySectionPrivate::getNumberOfElements).sum();
	}

	private ExceptionThread fillSection(Iterator<? extends CharSequence> objects, long count,
			ProgressListener listener) {
		PipedCopyIterator<TypedByteString> datatypeIterator = new PipedCopyIterator<>();
		String name = filename.getFileName().toString();
		Map<ByteString, DictionarySectionPrivate> theObjects = Collections.synchronizedMap(this.objects);
		return new ExceptionThread(() -> {
			// object reader
			try {
				ByteString oldType = null;
				long block = count < 10 ? 1 : count / 10;
				long currentCount = 0;
				for (; objects.hasNext(); currentCount++) {
					ByteString next = (ByteString) objects.next();

					ByteString lit = (ByteString) LiteralsUtils.prefToLit(next);
					ByteString type = (ByteString) LiteralsUtils.getType(lit);

					if (currentCount % block == 0) {
						listener.notifyProgress((float) (currentCount * 100 / count), "Filling section");
					}

					if (oldType != null) {
						if (oldType.equals(type)) {
							datatypeIterator.addElement(
									new TypedByteString(oldType, (ByteString) LiteralsUtils.removeType(lit)));
							continue;
						} else {
							datatypeIterator.closePipe();
						}
					}
					oldType = type;

					datatypeIterator
							.addElement(new TypedByteString(oldType, (ByteString) LiteralsUtils.removeType(lit)));
				}
				datatypeIterator.closePipe();
				datatypeIterator.closePipe();
			} catch (Throwable e) {
				try {
					throw e;
				} finally {
					datatypeIterator.closePipe(e);
				}
			}
		}, "MultiSecSAsyncObjectReader").attach(new ExceptionThread(() -> {
			// datatype writer
			PeekIteratorImpl<TypedByteString> dataTypePeekIt = new PeekIteratorImpl<>(datatypeIterator);
			// section id to not having to write an URI on disk
			Map<ByteString, Long> sectionIds = new HashMap<>();

			// check that we have at least one element to read
			while (dataTypePeekIt.hasNext()) {
				ByteString type = dataTypePeekIt.peek().getType();
				Long sid = sectionIds.get(type);
				if (sid != null) {
					// check that the section wasn't already defined
					throw new IllegalArgumentException("type " + type + " is already defined");
				}
				// create a new id
				long sidNew = 1L + sectionIds.size();
				sectionIds.put(type, sidNew);

				// create the new section
				WriteDictionarySection section = new WriteDictionarySection(spec,
						filename.resolveSibling(name + "type" + sidNew), bufferSize);
				theObjects.put(type, section);
				section.load(dataTypePeekIt.map(TypedByteString::getNode), count, null);

				// reset the pipe to allow reading more elements
				((PipedCopyIterator<?>) dataTypePeekIt.getWrappedIterator()).reset();
			}
		}, "MultiSecSAsyncObjectDatatypeWriter"));
	}

	@Override
	public void loadAsync(TempDictionary other, ProgressListener listener) throws InterruptedException {
		MultiThreadListener ml = ListenerUtil.multiThreadListener(listener);
		ml.unregisterAllThreads();
		ExceptionThread
				.async("MultiSecSAsyncReader",
						() -> predicates.load(other.getPredicates(), new IntermediateListener(ml, "Predicate: ")),
						() -> subjects.load(other.getSubjects(), new IntermediateListener(ml, "Subjects:  ")),
						() -> shared.load(other.getShared(), new IntermediateListener(ml, "Shared:    ")))
				.attach(fillSection(other.getObjects().getEntries(), other.getObjects().getNumberOfElements(),
						new IntermediateListener(ml, "Objects:   ")))
				.startAll().joinAndCrashIfRequired();
		ml.unregisterAllThreads();
	}

	@Override
	public void save(OutputStream output, ControlInfo ci, ProgressListener listener) throws IOException {
		ci.setType(ControlInfo.Type.DICTIONARY);
		ci.setFormat(getType());
		ci.setInt("elements", this.getNumberOfElements());
		ci.save(output);

		IntermediateListener iListener = new IntermediateListener(listener);
		iListener.setRange(0, 25);
		iListener.setPrefix("Save shared: ");
		shared.save(output, iListener);
		iListener.setRange(25, 50);
		iListener.setPrefix("Save subjects: ");
		subjects.save(output, iListener);
		iListener.setRange(50, 75);
		iListener.setPrefix("Save predicates: ");
		predicates.save(output, iListener);
		iListener.setRange(75, 100);
		iListener.setPrefix("Save objects: ");

		VByte.encode(output, objects.size());

		for (Map.Entry<ByteString, DictionarySectionPrivate> entry : objects.entrySet()) {
			IOUtil.writeSizedBuffer(output, entry.getKey(), listener);
		}

		for (Map.Entry<ByteString, DictionarySectionPrivate> entry : objects.entrySet()) {
			entry.getValue().save(output, iListener);
		}

	}

	@Override
	public void close() throws IOException {
		try {
			IOUtil.closeAll(shared, subjects, predicates);
		} finally {
			IOUtil.closeAll(objects.values());
		}
	}

	@Override
	public void populateHeader(Header header, String rootNode) {
		header.insert(rootNode, HDTVocabulary.DICTIONARY_TYPE, getType());
		header.insert(rootNode, HDTVocabulary.DICTIONARY_NUMSHARED, getNshared());
		header.insert(rootNode, HDTVocabulary.DICTIONARY_SIZE_STRINGS, size());
	}

	@Override
	public String getType() {
		return HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION;
	}

	@Override
	public void load(InputStream input, ControlInfo ci, ProgressListener listener) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public void mapFromFile(CountInputStream in, File f, ProgressListener listener) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public void load(TempDictionary other, ProgressListener listener) {
		throw new NotImplementedException();
	}

	private static class TypedByteString {
		private final ByteString type;
		private final ByteString node;

		public TypedByteString(ByteString type, ByteString node) {
			this.type = type;
			this.node = node;
		}

		public ByteString getNode() {
			return node;
		}

		public ByteString getType() {
			return type;
		}
	}
}
