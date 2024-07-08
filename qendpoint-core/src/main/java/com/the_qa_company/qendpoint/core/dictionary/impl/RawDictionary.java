package com.the_qa_company.qendpoint.core.dictionary.impl;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionary;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.DecimalDictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.DictionarySectionFactory;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.FloatDictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.IntDictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.OneReadDictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.PFCDictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.PFCDictionarySectionBig;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.iterator.charsequence.StopIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.CatIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.MapIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.PeekIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.StringQuotesSuffixIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.StringSuffixIterator;
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
import com.the_qa_company.qendpoint.core.util.string.RawStringUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Predicate;

import static java.lang.String.format;

public class RawDictionary extends MultipleLangBaseDictionary {

	public RawDictionary(HDTOptions spec) {
		this(spec, false);
	}

	public RawDictionary(HDTOptions spec, boolean quad) {
		super(spec);
		// FIXME: Read type from spec.
		subjects = new PFCDictionarySectionBig(spec);
		predicates = new PFCDictionarySectionBig(spec);
		Comparator<CharSequence> cmp = CharSequenceComparator.getInstance();
		typed = new TreeMap<>(cmp);
		languages = new TreeMap<>(cmp);
		nonTyped = new PFCDictionarySectionBig(spec);
		shared = new PFCDictionarySectionBig(spec);
		if (quad) {
			graph = new PFCDictionarySectionBig(spec);
		}
	}

	public RawDictionary(HDTOptions spec, DictionarySectionPrivate subjects, DictionarySectionPrivate predicates,
			DictionarySectionPrivate nonTyped, TreeMap<ByteString, DictionarySectionPrivate> typed,
			TreeMap<ByteString, DictionarySectionPrivate> languages, DictionarySectionPrivate shared) {
		this(spec, subjects, predicates, nonTyped, typed, languages, shared, null);
	}

	public RawDictionary(HDTOptions spec, DictionarySectionPrivate subjects, DictionarySectionPrivate predicates,
			DictionarySectionPrivate nonTyped, TreeMap<ByteString, DictionarySectionPrivate> typed,
			TreeMap<ByteString, DictionarySectionPrivate> languages, DictionarySectionPrivate shared,
			DictionarySectionPrivate graph) {
		super(spec);
		this.subjects = subjects;
		this.predicates = predicates;
		this.typed = typed;
		this.languages = languages;
		this.nonTyped = Objects.requireNonNullElseGet(nonTyped, () -> new PFCDictionarySection(spec));
		this.shared = shared;
		this.graph = graph;
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

		if (supportGraphs()) {
			graph = DictionarySectionFactory.loadFrom(input, iListener);
		}

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

		if (supportGraphs()) {
			graph = DictionarySectionFactory.loadFrom(in, f, iListener);
		}

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
				ByteString bsType = RawStringUtils.rawKnownDataType((ByteString) type);

				count = literalsCounts.get(bsType);
				Iterator<? extends CharSequence> stopIt;

				DictionarySectionPrivate section;
				if (bsType == RawStringUtils.XSD_DECIMAL_DT) {
					section = new DecimalDictionarySection(spec);
					stopIt = StopIterator.count(it, count).map(LiteralsUtils::removeQuotesTypeAndLang);
				} else if (bsType == RawStringUtils.XSD_DOUBLE_DT) {
					section = new FloatDictionarySection(spec);
					stopIt = StopIterator.count(it, count).map(LiteralsUtils::removeQuotesTypeAndLang);
				} else if (bsType == RawStringUtils.XSD_INTEGER_DT) {
					section = new IntDictionarySection(spec);
					stopIt = StopIterator.count(it, count).map(LiteralsUtils::removeQuotesTypeAndLang);
				} else {
					section = new PFCDictionarySectionBig(spec);
					stopIt = StopIterator.count(it, count).map(LiteralsUtils::removeType);
				}
				section.load(stopIt, count, listener);
				DictionarySectionPrivate old = typed.put(bsType, section);
				assert old == null : "section already def for " + first;
			}

			if (count == 0) {
				throw new RuntimeException("Can't find section for " + first);
			}
		}

		shared.load(other.getShared(), iListener);
		if (supportGraphs()) {
			graph.load(other.getGraphs(), iListener);
		}
		syncLocations();
	}

	private TempDictionarySection extractStrings(TempDictionarySection sec) {
		return new OneReadDictionarySection(
				MapIterator.of(sec.getSortedEntries(), RawStringUtils::convertFromRawString),
				sec.getNumberOfElements());
	}

	@Override
	public void loadAsync(TempDictionary other, ProgressListener listener) throws InterruptedException {
		IntermediateListener iListener = new IntermediateListener(null);
		new ExceptionThread(() -> predicates.load(extractStrings(other.getPredicates()), iListener),
				"MultiSecSAsyncReaderP")
				.attach(new ExceptionThread(() -> subjects.load(extractStrings(other.getSubjects()), iListener),
						"MultiSecSAsyncReaderS"),
						new ExceptionThread(() -> shared.load(extractStrings(other.getShared()), iListener),
								"MultiSecSAsyncReaderSh"),
						new ExceptionThread(() -> {
							if (supportGraphs()) {
								graph.load(extractStrings(other.getGraphs()), iListener);
							}
						}, "MultiSecSAsyncReaderG"), new ExceptionThread(() -> {
							RawStopPredicate<CharSequence> pred = new RawStopPredicate<>();
							PeekIterator<? extends CharSequence> it = new com.the_qa_company.qendpoint.core.iterator.utils.StopIterator<>(
									other.getObjects().getSortedEntries(), pred);

							while (it.hasNext()) {
								ByteString peek = ByteString.of(it.peek());

								ByteString type = RawStringUtils.rawType(peek);
								if (LiteralsUtils.NO_DATATYPE == type) {
									long count = other.getObjects().getNumberOfElements()
											- shared.getNumberOfElements();
									nonTyped.load(it.map(RawStringUtils::convertFromRawStringLitOnly), count, listener);
									pred.reset();
									continue;
								}

								if (LiteralsUtils.LITERAL_LANG_TYPE == type) {
									DictionarySectionPrivate section = new PFCDictionarySectionBig(spec);
									section.load(it.map(RawStringUtils::convertFromRawStringLitOnly), 1, listener);
									pred.reset();
									languages.put(RawStringUtils.getRawLang(peek), section);
									continue;
								}

								DictionarySectionPrivate section;
								if (type == RawStringUtils.XSD_DECIMAL_DT) {
									section = new DecimalDictionarySection(spec);
								} else if (type == RawStringUtils.XSD_DOUBLE_DT) {
									section = new FloatDictionarySection(spec);
								} else if (type == RawStringUtils.XSD_INTEGER_DT) {
									section = new IntDictionarySection(spec);
								} else {
									section = new PFCDictionarySectionBig(spec);
								}
								section.load(it.map(RawStringUtils::convertFromRawStringLitOnly), 1, listener);
								pred.reset();
								typed.put(type, section);
							}
						}, "MultiSecSAsyncReaderO"))
				.startAll().joinAndCrashIfRequired();
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
		if (supportGraphs()) {
			graph.save(output, iListener);
		}

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

	@Override
	public CharSequence idToString(long id, TripleComponentRole position) {
		switch (position) {
		case PREDICATE -> {
			return predicates.extract(id);
		}
		case SUBJECT -> {
			if (id <= shared.getNumberOfElements()) {
				return shared.extract(id);
			} else {
				return subjects.extract(id - shared.getNumberOfElements());
			}
		}
		case OBJECT -> {
			ObjectIdLocationData data = idToObjectSection(id);

			@SuppressWarnings("resource")
			DictionarySectionPrivate sec = data.section();
			CharSequence out = sec.extract(id - data.location());
			if (out != null) {
				ByteString obs = ByteString.of(out);
				if (sec.getSectionType().hasQuotes()) {
					return data.suffix().copyPreAppend(obs);
				}
				ByteString suffix = data.suffix();
				byte[] buffer = new byte[suffix.length() + obs.length() + 2];

				buffer[0] = '"';
				System.arraycopy(obs.getBuffer(), 0, buffer, 1, obs.length());
				buffer[obs.length() + 1] = '"';
				System.arraycopy(suffix.getBuffer(), 0, buffer, obs.length() + 2, suffix.length());

				return new CompactString(buffer);
			}

			return null;
		}
		case GRAPH -> {
			return graph.extract(id);
		}
		default -> throw new NotImplementedException();
		}
	}

	@Override
	public long stringToId(CharSequence sstr, TripleComponentRole position) {
		if (sstr == null || sstr.length() == 0) {
			return 0;
		}
		ByteString str = ByteString.of(sstr);

		switch (position) {
		case PREDICATE -> {
			long id = predicates.locate(str);
			return id > 0 ? id : -1;
		}
		case GRAPH -> {
			if (!supportGraphs()) {
				throw new IllegalArgumentException("This dictionary doesn't support graphs!");
			}
			long id = graph.locate(str);
			return id > 0 ? id : -1;
		}
		case SUBJECT -> {
			long sid = shared.locate(str);
			if (sid != 0) {
				return sid;
			}

			long ssid = subjects.locate(str);
			if (ssid != 0) {
				return ssid + shared.getNumberOfElements();
			}
		}
		case OBJECT -> {
			CharSequence t = LiteralsUtils.getType(str);

			if (LiteralsUtils.NO_DATATYPE == t) {
				long sid = shared.locate(str);
				if (sid != 0) {
					return sid;
				}
			}

			if (LiteralsUtils.LITERAL_LANG_TYPE == t) {
				// lang type
				ByteString lang = ByteString.of(LiteralsUtils.getLanguage(str)
						.orElseThrow(() -> new IllegalArgumentException("Malformed language literal " + str)));

				ObjectIdLocationData sec = languagesLocations.get(lang);
				if (sec != null) {
					CharSequence nl = LiteralsUtils.removeLang(str);

					@SuppressWarnings("resource")
					long s = sec.section().locate(nl);
					if (s != 0) {
						return sec.location() + s;
					}
				}
				return -1;
			}

			ObjectIdLocationData sec = objectsLocations.get((ByteString) t);

			if (sec == null) {
				return -1;
			}

			@SuppressWarnings("resource")
			DictionarySectionPrivate section = sec.section();
			long s = section.locate(section.getSectionType().hasQuotes() ? LiteralsUtils.removeType(str)
					: LiteralsUtils.removeQuotesTypeAndLang(str));

			if (s == 0) {
				return -1;
			}
			return sec.location() + s;

		}
		default -> throw new NotImplementedException();
		}
		return -1;
	}

	@Override
	public Iterator<? extends CharSequence> stringIterator(TripleComponentRole role, boolean includeShared) {
		switch (role) {
		case SUBJECT -> {
			if (!includeShared) {
				return getSubjects().getSortedEntries();
			}

			return CatIterator.of(getShared().getSortedEntries(), getSubjects().getSortedEntries());
		}
		case PREDICATE -> {
			return getPredicates().getSortedEntries();
		}
		case OBJECT -> {
			return CatIterator.of(Arrays.stream(objectIdLocationsSec).skip(includeShared ? 0 : 1).map(data -> {
				ByteString suffix = data.suffix();
				@SuppressWarnings("resource")
				DictionarySectionPrivate sec = data.section();
				if (sec.getSectionType().hasQuotes()) {
					return StringSuffixIterator.of(sec.getSortedEntries(), suffix);
				} else {
					return StringQuotesSuffixIterator.of(sec.getSortedEntries(), suffix);
				}
			}).toList());
		}
		case GRAPH -> {
			if (!supportGraphs()) {
				throw new IllegalArgumentException("This dictionary doesn't support graphs!");
			}
			return getGraphs().getSortedEntries();
		}
		default -> throw new IllegalArgumentException("Unknown role: " + role);
		}
	}

	protected static class RawStopPredicate<T extends CharSequence> implements Predicate<T> {
		private CharSequence type;

		@Override
		public boolean test(T charSequence) {
			ByteString type = RawStringUtils.rawDTLType(ByteString.of(charSequence));
			if (this.type == null) {
				this.type = type;
				return true;
			}
			return this.type.equals(type);
		}

		public void reset() {
			this.type = null;
		}
	}

	@Override
	public String getType() {
		return HDTVocabulary.DICTIONARY_TYPE_RAW;
	}
}
