package com.the_qa_company.qendpoint.core.dictionary.impl;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.DictionarySectionFactory;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.PFCDictionarySection;
import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.header.Header;
import com.the_qa_company.qendpoint.core.iterator.utils.MapIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.PeekIteratorImpl;
import com.the_qa_company.qendpoint.core.iterator.utils.StopIterator;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.ControlInfo;
import com.the_qa_company.qendpoint.core.options.ControlInformation;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.util.CustomIterator;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import com.the_qa_company.qendpoint.core.util.concurrent.ExceptionThread;
import com.the_qa_company.qendpoint.core.util.io.CountInputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.listener.IntermediateListener;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.CharSequenceComparator;
import com.the_qa_company.qendpoint.core.util.string.CompactString;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class MultipleSectionDictionary extends MultipleBaseDictionary {

	public MultipleSectionDictionary(HDTOptions spec) {
		super(spec);
		// FIXME: Read type from spec.
		subjects = new PFCDictionarySection(spec);
		predicates = new PFCDictionarySection(spec);
		objects = new TreeMap<>(CharSequenceComparator.getInstance());
		shared = new PFCDictionarySection(spec);
	}

	public MultipleSectionDictionary(HDTOptions spec, DictionarySectionPrivate subjects,
			DictionarySectionPrivate predicates, TreeMap<ByteString, DictionarySectionPrivate> objects,
			DictionarySectionPrivate shared) {
		super(spec);
		this.subjects = subjects;
		this.predicates = predicates;
		this.objects = objects;
		this.shared = shared;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.Dictionary#load(hdt.dictionary.Dictionary)
	 */
	@Override
	public void load(TempDictionary other, ProgressListener listener) {
		IntermediateListener iListener = new IntermediateListener(listener);
		subjects.load(other.getSubjects(), iListener);
		predicates.load(other.getPredicates(), iListener);
		Iterator<? extends CharSequence> iter = other.getObjects().getEntries();

		Map<ByteString, Long> literalsCounts = new HashMap<>(other.getObjects().getLiteralsCounts());
		literalsCounts.computeIfPresent(LiteralsUtils.NO_DATATYPE,
				(key, value) -> (value - other.getShared().getNumberOfElements()));
		CustomIterator customIterator = new CustomIterator(iter, literalsCounts, false);
		while (customIterator.hasNext()) {
			DictionarySectionPrivate section = DictionarySectionFactory.createDictionarySection(spec);
			ByteString type = ByteString.of(LiteralsUtils.getType(customIterator.prev));
			long numEntries = literalsCounts.get(type);

			section.load(customIterator, numEntries, listener);
			objects.put(type, section);
		}
		shared.load(other.getShared(), iListener);
	}

	@Override
	public void loadAsync(TempDictionary other, ProgressListener listener) throws InterruptedException {
		IntermediateListener iListener = new IntermediateListener(null);
		new ExceptionThread(() -> predicates.load(other.getPredicates(), iListener), "MultiSecSAsyncReaderP").attach(
				new ExceptionThread(() -> subjects.load(other.getSubjects(), iListener), "MultiSecSAsyncReaderS"),
				new ExceptionThread(() -> shared.load(other.getShared(), iListener), "MultiSecSAsyncReaderSh"),
				new ExceptionThread(() -> {
					StopPredicate<CharSequence> pred = new StopPredicate<>();
					PeekIteratorImpl<? extends CharSequence> it = new PeekIteratorImpl<>(
							new StopIterator<>(new MapIterator<>(other.getObjects().getSortedEntries(),
									b -> LiteralsUtils.prefToLit(ByteString.of(b))), pred));

					while (it.hasNext()) {
						PFCDictionarySection section = new PFCDictionarySection(spec);
						ByteString type = (ByteString) (LiteralsUtils.getType(it.peek()));
						long count;
						if (LiteralsUtils.isNoDatatype(type)) {
							count = other.getObjects().getNumberOfElements() - shared.getNumberOfElements();
						} else {
							// don't know the count
							count = 1;
						}
						section.load(it.map(LiteralsUtils::removeType), count, listener);
						pred.reset();
						objects.put(type, section);
					}
				}, "MultiSecSAsyncReaderO")).startAll().joinAndCrashIfRequired();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.Dictionary#save(java.io.OutputStream,
	 * hdt.ControlInformation, hdt.ProgressListener)
	 */
	@Override
	public void save(OutputStream output, ControlInfo ci, ProgressListener listener) throws IOException {
		ci.setType(ControlInfo.Type.DICTIONARY);
		ci.setFormat(getType());
		ci.setInt("elements", this.getNumberOfElements());
		ci.save(output);

		IntermediateListener iListener = new IntermediateListener(listener);
		shared.save(output, iListener);
		subjects.save(output, iListener);
		predicates.save(output, iListener);

		writeLiteralsMap(output, iListener);

	}

	/*
	 * ------------------ |len| Literal URI| ------------------
	 */
	private void writeLiteralsMap(OutputStream output, ProgressListener listener) throws IOException {
		Iterator<Map.Entry<ByteString, DictionarySectionPrivate>> hmIterator = objects.entrySet().iterator();
		int numberOfTypes = objects.size();
		VByte.encode(output, numberOfTypes);

		List<ByteString> types = new ArrayList<>();

		while (hmIterator.hasNext()) {
			Map.Entry<ByteString, DictionarySectionPrivate> entry = hmIterator.next();
			ByteString uri = entry.getKey();
			IOUtil.writeSizedBuffer(output, uri.getBuffer(), 0, uri.length(), listener);
			types.add(uri);
		}
		for (ByteString type : types) {
			this.objects.get(type).save(output, listener);
		}
	}

	private void readLiteralsMap(InputStream input, ProgressListener listener) throws IOException {
		int numberOfTypes = (int) VByte.decode(input);
		List<ByteString> types = new ArrayList<>();
		for (int i = 0; i < numberOfTypes; i++) {
			int length = (int) VByte.decode(input);
			byte[] type = IOUtil.readBuffer(input, length, listener);
			types.add(new CompactString(type));
		}
		for (ByteString type : types) {
			this.objects.put(type, DictionarySectionFactory.loadFrom(input, listener));
		}
	}

	private void mapLiteralsMap(CountInputStream input, File f, ProgressListener listener) throws IOException {
		input.printIndex("objects");
		int numberOfTypes = (int) VByte.decode(input);
		List<ByteString> types = new ArrayList<>();
		for (int i = 0; i < numberOfTypes; i++) {
			int length = (int) VByte.decode(input);
			byte[] type = IOUtil.readBuffer(input, length, listener);
			types.add(new CompactString(type));
		}
		input.printIndex("sections");
		for (ByteString type : types) {
			input.printIndex("sections/" + type);
			this.objects.put(type, DictionarySectionFactory.loadFrom(input, f, listener));
		}

	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.Dictionary#load(java.io.InputStream)
	 */
	@Override
	public void load(InputStream input, ControlInfo ci, ProgressListener listener) throws IOException {
		if (ci.getType() != ControlInfo.Type.DICTIONARY) {
			throw new IllegalFormatException("Trying to read a dictionary section, but was not dictionary.");
		}

		IntermediateListener iListener = new IntermediateListener(listener);

		shared = DictionarySectionFactory.loadFrom(input, iListener);
		subjects = DictionarySectionFactory.loadFrom(input, iListener);
		predicates = DictionarySectionFactory.loadFrom(input, iListener);

		readLiteralsMap(input, listener);
	}

	@Override
	public void mapFromFile(CountInputStream in, File f, ProgressListener listener) throws IOException {
		ControlInformation ci = new ControlInformation();
		ci.load(in);
		if (ci.getType() != ControlInfo.Type.DICTIONARY) {
			throw new IllegalFormatException("Trying to read a dictionary section, but was not dictionary.");
		}

		IntermediateListener iListener = new IntermediateListener(listener);
		shared = DictionarySectionFactory.loadFrom(in, f, iListener);
		subjects = DictionarySectionFactory.loadFrom(in, f, iListener);
		predicates = DictionarySectionFactory.loadFrom(in, f, iListener);

		mapLiteralsMap(in, f, listener);

		// Use cache only for predicates. Preload only up to 100K predicates.
		// FIXME: DISABLED
//		predicates = new DictionarySectionCacheAll(predicates, predicates.getNumberOfElements()<100000);
	}

	@Override
	public long getNgraphs() {
		return 0;
	}

	@Override
	public DictionarySection getGraphs() {
		throw new NotImplementedException();
	}

	@Override
	public long getNAllObjects() {
		return objects.values().stream().mapToLong(DictionarySectionPrivate::getNumberOfElements).sum();
	}

	@Override
	public TreeMap<? extends CharSequence, DictionarySection> getAllObjects() {
		return new TreeMap<>(objects);
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.Dictionary#populateHeader(hdt.header.Header,
	 * java.lang.String)
	 */
	@Override
	public void populateHeader(Header header, String rootNode) {
		header.insert(rootNode, HDTVocabulary.DICTIONARY_TYPE, getType());
//		header.insert(rootNode, HDTVocabulary.DICTIONARY_NUMSUBJECTS, getNsubjects());
//		header.insert(rootNode, HDTVocabulary.DICTIONARY_NUMPREDICATES, getNpredicates());
//		header.insert(rootNode, HDTVocabulary.DICTIONARY_NUMOBJECTS, getNobjects());
		header.insert(rootNode, HDTVocabulary.DICTIONARY_NUMSHARED, getNshared());
//		header.insert(rootNode, HDTVocabulary.DICTIONARY_MAXSUBJECTID, getMaxSubjectID());
//		header.insert(rootNode, HDTVocabulary.DICTIONARY_MAXPREDICATEID, getMaxPredicateID());
//		header.insert(rootNode, HDTVocabulary.DICTIONARY_MAXOBJECTTID, getMaxObjectID());
		header.insert(rootNode, HDTVocabulary.DICTIONARY_SIZE_STRINGS, size());
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.Dictionary#getType()
	 */
	@Override
	public String getType() {
		return HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION;
	}

	@Override
	public void close() throws IOException {
		shared.close();
		subjects.close();
		predicates.close();

		// close all subsections
		IOUtil.closeAll(objects.values());
	}
}
