package com.the_qa_company.qendpoint.core.dictionary.impl;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.DictionarySectionFactory;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.PFCDictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.PFCDictionarySectionBig;
import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.iterator.charsequence.StopIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.MapIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.PeekIterator;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.ControlInfo;
import com.the_qa_company.qendpoint.core.options.ControlInformation;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import com.the_qa_company.qendpoint.core.util.concurrent.ExceptionThread;
import com.the_qa_company.qendpoint.core.util.io.CountInputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.listener.IntermediateListener;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.CharSequenceComparator;
import com.the_qa_company.qendpoint.core.util.string.CompactString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static java.lang.String.format;

public class MultipleSectionDictionaryLang extends MultipleLangBaseDictionary {
	private static final Logger logger = LoggerFactory.getLogger(MultipleSectionDictionaryLang.class);

	public MultipleSectionDictionaryLang(HDTOptions spec) {
		super(spec);
		// FIXME: Read type from spec.
		subjects = new PFCDictionarySectionBig(spec);
		predicates = new PFCDictionarySectionBig(spec);
		Comparator<CharSequence> cmp = CharSequenceComparator.getInstance();
		typed = new TreeMap<>(cmp);
		languages = new TreeMap<>(cmp);
		nonTyped = new PFCDictionarySectionBig(spec);
		shared = new PFCDictionarySectionBig(spec);
	}

	public MultipleSectionDictionaryLang(HDTOptions spec, DictionarySectionPrivate subjects,
			DictionarySectionPrivate predicates, DictionarySectionPrivate nonTyped,
			TreeMap<ByteString, DictionarySectionPrivate> typed,
			TreeMap<ByteString, DictionarySectionPrivate> languages, DictionarySectionPrivate shared) {
		super(spec);
		this.subjects = subjects;
		this.predicates = predicates;
		this.typed = typed;
		this.languages = languages;
		this.nonTyped = Objects.requireNonNullElseGet(nonTyped, () -> new PFCDictionarySection(spec));
		this.shared = shared;
		syncLocations();
	}

	@Override
	public void load(InputStream input, ControlInfo ci, ProgressListener listener) throws IOException {
		if (ci.getType() != ControlInfo.Type.DICTIONARY) {
			throw new IllegalFormatException("Trying to read a dictionary section, but was not dictionary.");
		}

		IntermediateListener iListener = new IntermediateListener(listener);

		shared = DictionarySectionFactory.loadFrom(input, iListener);
		subjects = DictionarySectionFactory.loadFrom(input, iListener);
		predicates = DictionarySectionFactory.loadFrom(input, iListener);
		nonTyped = DictionarySectionFactory.loadFrom(input, iListener);

		readLiteralsMaps(input, listener);
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
		nonTyped = DictionarySectionFactory.loadFrom(in, f, iListener);

		mapLiteralsMaps(in, f, listener);
	}

	@Override
	public void load(TempDictionary other, ProgressListener listener) {
		IntermediateListener iListener = new IntermediateListener(listener);
		subjects.load(other.getSubjects(), iListener);
		predicates.load(other.getPredicates(), iListener);
		Map<ByteString, Long> literalsCounts = other.getObjects().getLiteralsCounts();
		PeekIterator<? extends CharSequence> it = PeekIterator.of(other.getObjects().getEntries());

		boolean nonTypedLoaded = false;
		while (it.hasNext()) {
			ByteString first = ByteString.of(it.peek());

			long count;
			CharSequence type = LiteralsUtils.getType(first);
			if (type == LiteralsUtils.NO_DATATYPE) {
				count = literalsCounts.get(type) - other.getShared().getNumberOfElements();
				Iterator<? extends CharSequence> stopIt = StopIterator.count(it, count);
				nonTyped.load(stopIt, count, listener);
				assert !nonTypedLoaded : "nonTypedLoaded for " + first;
				nonTypedLoaded = true;
			} else if (type == LiteralsUtils.LITERAL_LANG_TYPE) {
				ByteString language = (ByteString) LiteralsUtils.getLanguage(first)
						.orElseThrow(() -> new RuntimeException("Find language literal without language! " + first));
				count = literalsCounts.get(LiteralsUtils.LANG_OPERATOR.copyAppend(language));
				Iterator<? extends CharSequence> stopIt = StopIterator.count(it, count).map(LiteralsUtils::removeLang);

				PFCDictionarySectionBig section = new PFCDictionarySectionBig(spec);
				section.load(stopIt, count, listener);
				DictionarySectionPrivate old = languages.put(language, section);
				assert old == null : "section already def for " + first;
			} else {
				ByteString bsType = (ByteString) type;

				count = literalsCounts.get(bsType);
				Iterator<? extends CharSequence> stopIt = StopIterator.count(it, count).map(LiteralsUtils::removeType);

				PFCDictionarySectionBig section = new PFCDictionarySectionBig(spec);
				section.load(stopIt, count, listener);
				DictionarySectionPrivate old = typed.put(bsType, section);
				assert old == null : "section already def for " + first;
			}

			if (count == 0) {
				throw new RuntimeException("Can't find section for " + first);
			}
		}

		shared.load(other.getShared(), iListener);
		syncLocations();
	}

	@Override
	public void loadAsync(TempDictionary other, ProgressListener listener) throws InterruptedException {
		IntermediateListener iListener = new IntermediateListener(null);
		new ExceptionThread(() -> predicates.load(other.getPredicates(), iListener), "MultiSecSAsyncReaderP").attach(
				new ExceptionThread(() -> subjects.load(other.getSubjects(), iListener), "MultiSecSAsyncReaderS"),
				new ExceptionThread(() -> shared.load(other.getShared(), iListener), "MultiSecSAsyncReaderSh"),
				new ExceptionThread(() -> {
					StopPredicate<CharSequence> pred = new StopPredicate<>();
					PeekIterator<? extends CharSequence> it = new com.the_qa_company.qendpoint.core.iterator.utils.StopIterator<>(
							new MapIterator<>(other.getObjects().getSortedEntries(), LiteralsUtils::prefToLitLang),
							pred);

					while (it.hasNext()) {
						CharSequence peek = it.peek();
						ByteString type = (ByteString) (LiteralsUtils.getType(peek));
						if (LiteralsUtils.NO_DATATYPE == type) {
							long count = other.getObjects().getNumberOfElements() - shared.getNumberOfElements();
							nonTyped.load(it, count, listener);
							pred.reset();
							continue;
						}
						PFCDictionarySectionBig section = new PFCDictionarySectionBig(spec);
						section.load(it.map(LiteralsUtils::removeTypeAndLang), 1, listener);
						pred.reset();

						if (LiteralsUtils.LITERAL_LANG_TYPE == type) {
							languages.put((ByteString) LiteralsUtils.getLanguage(peek).orElseThrow(), section);
						} else {
							typed.put(type, section);
						}
					}
				}, "MultiSecSAsyncReaderO")).startAll().joinAndCrashIfRequired();
		syncLocations();
	}

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
		nonTyped.save(output, iListener);

		writeLiteralsMaps(output, iListener);
	}

	private void writeLiteralsMaps(OutputStream output, ProgressListener listener) throws IOException {
		VByte.encode(output, typed.size() + languages.size());

		for (var e : typed.entrySet()) {
			ByteString uriKey = e.getKey();
			DictionarySectionPrivate sec = e.getValue();

			output.write(SECTION_TYPE_DT);
			IOUtil.writeSizedBuffer(output, uriKey, listener);
			sec.save(output, listener);
		}
		for (var e : languages.entrySet()) {
			ByteString uriKey = e.getKey();
			DictionarySectionPrivate sec = e.getValue();

			output.write(SECTION_TYPE_LANG);
			IOUtil.writeSizedBuffer(output, uriKey, listener);
			sec.save(output, listener);
		}
	}

	private void readLiteralsMaps(InputStream input, ProgressListener listener) throws IOException {
		int numberOfTypes = (int) VByte.decode(input);

		for (int i = 0; i < numberOfTypes; i++) {
			int type = input.read();
			Map<ByteString, DictionarySectionPrivate> location = switch (type) {
			case SECTION_TYPE_DT -> typed;
			case SECTION_TYPE_LANG -> languages;
			default -> throw new IOException(format("Find bad literal section type %x", type));
			};
			ByteString uriKey = new CompactString(IOUtil.readSizedBuffer(input, listener));
			DictionarySectionPrivate old = location.put(uriKey, DictionarySectionFactory.loadFrom(input, listener));

			if (old != null) {
				old.close(); // wtf?
			}
		}

		syncLocations();
	}

	private void mapLiteralsMaps(CountInputStream input, File f, ProgressListener listener) throws IOException {
		int numberOfTypes = (int) VByte.decode(input);

		for (int i = 0; i < numberOfTypes; i++) {
			int type = input.read();
			Map<ByteString, DictionarySectionPrivate> location = switch (type) {
			case SECTION_TYPE_DT -> typed;
			case SECTION_TYPE_LANG -> languages;
			default -> throw new IOException(format("Find bad literal section type %x", type));
			};
			ByteString uriKey = new CompactString(IOUtil.readSizedBuffer(input, listener));
			DictionarySectionPrivate old = location.put(uriKey, DictionarySectionFactory.loadFrom(input, f, listener));

			if (old != null) {
				old.close(); // wtf?
			}
		}

		syncLocations();
	}
}
