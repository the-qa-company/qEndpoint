package com.the_qa_company.qendpoint.core.dictionary.impl;

import com.the_qa_company.qendpoint.core.dictionary.DictionaryPrivate;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.enums.RDFNodeType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.header.Header;
import com.the_qa_company.qendpoint.core.iterator.utils.CatIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.StringSuffixIterator;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import com.the_qa_company.qendpoint.core.util.SortedDictionarySectionIndex;
import com.the_qa_company.qendpoint.core.util.disk.LongArray;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.CharSequenceComparator;
import com.the_qa_company.qendpoint.core.util.string.CharSequenceDTLComparator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;

public abstract class MultipleLangBaseDictionary implements DictionaryPrivate {
	protected enum ObjectIdLocationType {
		SHARED, NON_TYPED, TYPE, LANGUAGE
	}

	public record ObjectIdLocationData(int uid, ByteString name, ByteString suffix, DictionarySectionPrivate section,
			ObjectIdLocationType type, long location) {}

	/**
	 * byte to describe a datatype object
	 */
	public static final byte SECTION_TYPE_DT = 0x10;
	/**
	 * byte to describe a lang object
	 */
	public static final byte SECTION_TYPE_LANG = 0x11;
	protected final HDTOptions spec;
	protected final boolean noRdfTypeIndex;

	// sub sections
	protected DictionarySectionPrivate subjects;
	protected DictionarySectionPrivate predicates;

	protected DictionarySectionPrivate nonTyped;
	protected SortedDictionarySectionIndex nonTypedIndex;
	protected SortedDictionarySectionIndex subjectResIndex;
	protected SortedDictionarySectionIndex sharedIndex;
	protected SortedDictionarySectionIndex graphIndex;
	protected TreeMap<ByteString, DictionarySectionPrivate> languages;
	protected TreeMap<ByteString, DictionarySectionPrivate> typed;
	protected TreeMap<ByteString, ObjectIdLocationData> objectsLocations;
	protected TreeMap<ByteString, ObjectIdLocationData> languagesLocations;
	protected DictionarySectionPrivate shared;
	protected DictionarySectionPrivate graph;

	// locations
	protected LongArray objectIdLocations = LongArray.of(0);
	protected long typedLiteralsStart;
	protected ObjectIdLocationData[] objectIdLocationsSec = new ObjectIdLocationData[0];

	public MultipleLangBaseDictionary(HDTOptions spec) {
		this.spec = HDTOptions.ofNullable(spec);
		noRdfTypeIndex = this.spec.getBoolean(HDTOptionsKeys.DICTIONARY_MSDL_NO_RDFTYPE_INDEX, false);
	}

	@Override
	public long getNAllObjects() {
		return shared.getNumberOfElements() + nonTyped.getNumberOfElements()
				+ typed.values().stream().mapToLong(DictionarySection::getNumberOfElements).sum()
				+ languages.values().stream().mapToLong(DictionarySection::getNumberOfElements).sum();
	}

	@Override
	public void populateHeader(Header header, String rootNode) {
		header.insert(rootNode, HDTVocabulary.DICTIONARY_TYPE, getType());
		header.insert(rootNode, HDTVocabulary.DICTIONARY_NUMSHARED, getNshared());
		header.insert(rootNode, HDTVocabulary.DICTIONARY_SIZE_STRINGS, size());
	}

	@Override
	public String getType() {
		if (supportGraphs()) {
			return HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG_QUAD;
		}
		return HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG;
	}

	@Override
	public long getNumberOfElements() {
		return subjects.getNumberOfElements() + nonTyped.getNumberOfElements() + predicates.getNumberOfElements()
				+ shared.getNumberOfElements()
				+ languages.values().stream().mapToLong(DictionarySection::getNumberOfElements).sum()
				+ typed.values().stream().mapToLong(DictionarySection::getNumberOfElements).sum();
	}

	@Override
	public long size() {
		return subjects.size() + predicates.size() + shared.size() + nonTyped.size()
				+ languages.values().stream().mapToLong(DictionarySection::size).sum()
				+ typed.values().stream().mapToLong(DictionarySection::size).sum();
	}

	@Override
	public long getNsubjects() {
		return subjects.getNumberOfElements() + shared.getNumberOfElements();
	}

	@Override
	public long getNpredicates() {
		return predicates.getNumberOfElements();
	}

	@Override
	public long getNobjects() {
		return getNshared() + nonTyped.getNumberOfElements()
				+ languages.values().stream().mapToLong(DictionarySection::getNumberOfElements).sum()
				+ typed.values().stream().mapToLong(DictionarySection::getNumberOfElements).sum();
	}

	@Override
	public long getNshared() {
		return shared.getNumberOfElements();
	}

	@Override
	public long getNgraphs() {
		return graph.getNumberOfElements();
	}

	@Override
	public DictionarySection getSubjects() {
		return subjects;
	}

	@Override
	public DictionarySection getPredicates() {
		return predicates;
	}

	@Override
	public DictionarySection getObjects() {
		throw new NotImplementedException();
	}

	@Override
	public DictionarySectionPrivate getGraphs() {
		return graph;
	}

	@Override
	public Map<? extends CharSequence, DictionarySection> getAllObjects() {
		TreeMap<CharSequence, DictionarySection> m = new TreeMap<>(CharSequenceComparator.getInstance());
		m.put(LiteralsUtils.NO_DATATYPE, nonTyped);
		m.putAll(typed);
		languages.forEach((s, ls) -> m.put(LiteralsUtils.LANG_OPERATOR.copyAppend(s), ls));
		return m;
	}

	@Override
	public DictionarySection getShared() {
		return shared;
	}

	/**
	 * sync the ids of each object subsections
	 */
	protected void syncLocations() {
		// objectIdLocationsSec
		// objectIdLocationsSuffix
		// objectIdLocations
		// using default LongArray impl and not a Sequence because the arrays
		// are too small to create a memory diff
		objectsLocations = new TreeMap<>(CharSequenceComparator.getInstance());
		languagesLocations = new TreeMap<>(CharSequenceComparator.getInstance());

		int sectionCount = 0;

		if (getNshared() > 0) {
			sectionCount++;
		}

		sectionCount += (int) getAllObjects().values().stream().filter(d -> d.getNumberOfElements() != 0).count();

		objectIdLocationsSec = new ObjectIdLocationData[sectionCount];
		objectIdLocations = LongArray.of(objectIdLocationsSec.length);

		long count = 0;
		int id = 0;
		if (shared.getNumberOfElements() > 0) {
			objectIdLocationsSec[id] = new ObjectIdLocationData(id, ByteString.empty(), ByteString.empty(), shared,
					ObjectIdLocationType.SHARED, count);
			count += shared.getNumberOfElements();
			objectIdLocations.set(id++, count);
		}

		if (nonTyped.getNumberOfElements() > 0) {
			ObjectIdLocationData iodlocdObj = new ObjectIdLocationData(id, LiteralsUtils.NO_DATATYPE,
					ByteString.empty(), nonTyped, ObjectIdLocationType.NON_TYPED, count);
			objectsLocations.put(LiteralsUtils.NO_DATATYPE, iodlocdObj);
			objectIdLocationsSec[id] = iodlocdObj;
			count += nonTyped.getNumberOfElements();
			objectIdLocations.set(id++, count);
		}

		typedLiteralsStart = count + 1;

		for (var e : typed.entrySet()) {
			ByteString dt = e.getKey();
			DictionarySectionPrivate sec = e.getValue();

			if (sec.getNumberOfElements() == 0) {
				continue;
			}

			ObjectIdLocationData oidlocd = new ObjectIdLocationData(id, dt, LiteralsUtils.TYPE_OPERATOR.copyAppend(dt),
					sec, ObjectIdLocationType.TYPE, count);

			count += sec.getNumberOfElements();
			objectsLocations.put(dt, oidlocd);
			objectIdLocationsSec[id] = oidlocd;
			objectIdLocations.set(id++, count);
		}

		for (var e : languages.entrySet()) {
			ByteString dt = e.getKey();
			DictionarySectionPrivate sec = e.getValue();

			if (sec.getNumberOfElements() == 0) {
				continue;
			}

			ObjectIdLocationData iodlocd = new ObjectIdLocationData(id, dt, LiteralsUtils.LANG_OPERATOR.copyAppend(dt),
					sec, ObjectIdLocationType.LANGUAGE, count);

			count += sec.getNumberOfElements();
			objectIdLocationsSec[id] = iodlocd;
			languagesLocations.put(dt, iodlocd);
			objectIdLocations.set(id++, count);
		}

		assert id == objectIdLocationsSec.length;

		if (!noRdfTypeIndex) {
			nonTypedIndex = new SortedDictionarySectionIndex(nonTyped);
			subjectResIndex = new SortedDictionarySectionIndex(subjects);
			sharedIndex = new SortedDictionarySectionIndex(shared);
			if (supportGraphs()) {
				graphIndex = new SortedDictionarySectionIndex(graph);
			}
		}
	}

	public ObjectIdLocationData idToObjectSection(long id) {
		int location = (int) objectIdLocations.binarySearchLocation(id);

		if (location < 0 || location >= objectIdLocationsSec.length) {
			return null;
		}

		return objectIdLocationsSec[location];
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

			DictionarySectionPrivate sec = data.section;
			CharSequence out = sec.extract(id - data.location);
			if (out != null) {
				return data.suffix.copyPreAppend(out);
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
					long s = sec.section.locate(nl);
					if (s != 0) {
						return sec.location + s;
					}
				}
				return -1;
			}

			ObjectIdLocationData sec = objectsLocations.get((ByteString) t);

			if (sec == null) {
				return -1;
			}

			long s = sec.section.locate(LiteralsUtils.removeType(str));

			if (s == 0) {
				return -1;
			}
			return sec.location + s;

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
				ByteString suffix = data.suffix;
				DictionarySectionPrivate sec = data.section;
				return StringSuffixIterator.of(sec.getSortedEntries(), suffix);
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

	@Override
	public CharSequence dataTypeOfId(long id) {
		int location = (int) objectIdLocations.binarySearchLocation(id);

		if (location < 0 || location >= objectIdLocationsSec.length) {
			return LiteralsUtils.NO_DATATYPE;
		}

		ObjectIdLocationData data = objectIdLocationsSec[location];
		if (data.type != ObjectIdLocationType.TYPE) {
			if (data.type == ObjectIdLocationType.LANGUAGE) {
				return LiteralsUtils.LITERAL_LANG_TYPE;
			}
			return LiteralsUtils.NO_DATATYPE;
		}

		return data.name;
	}

	@Override
	public CharSequence languageOfId(long id) {
		int location = (int) objectIdLocations.binarySearchLocation(id);

		if (location < 0 || location >= objectIdLocationsSec.length) {
			return null;
		}

		ObjectIdLocationData data = objectIdLocationsSec[location];
		if (data.type != ObjectIdLocationType.LANGUAGE) {
			return null;
		}

		return data.name;
	}

	@Override
	public RDFNodeType nodeTypeOfId(TripleComponentRole role, long id) {
		if (noRdfTypeIndex) {
			return null;
		}
		if (id <= 0 || id > getNSection(role)) {
			return null;
		}
		switch (role) {
		case PREDICATE -> {
			return RDFNodeType.IRI;
		}
		case SUBJECT -> {
			long nshared = getNshared();
			if (id <= nshared) {
				return sharedIndex.getNodeType(id);
			}
			return subjectResIndex.getNodeType(id - nshared);
		}
		case OBJECT -> {
			if (id >= typedLiteralsStart) {
				// in typed or language
				return RDFNodeType.LITERAL;
			}
			long nshared = getNshared();
			if (id <= nshared) {
				return sharedIndex.getNodeType(id);
			}
			return nonTypedIndex.getNodeType(id - nshared);
		}
		case GRAPH -> {
			if (!supportGraphs()) {
				throw new IllegalArgumentException("This dictionary doesn't support graphs!");
			}
			return graphIndex.getNodeType(id);
		}
		}
		throw new IllegalArgumentException("Method is not applicable on z this dictionary");
	}

	@Override
	public boolean supportsNodeTypeOfId() {
		return !noRdfTypeIndex;
	}

	@Override
	public boolean supportsDataTypeOfId() {
		return true;
	}

	@Override
	public boolean supportsLanguageOfId() {
		return true;
	}

	@Override
	public boolean supportGraphs() {
		return graph != null;
	}

	@Override
	public void close() throws IOException {
		Closer.closeAll(subjects, predicates, nonTyped, typed, languages, shared, graph);
	}

	protected static class StopPredicate<T extends CharSequence> implements Predicate<T> {
		private CharSequence type;

		@Override
		public boolean test(T charSequence) {
			CharSequence type = CharSequenceDTLComparator.getDTLType(charSequence);
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
	public OptimizedExtractor createOptimizedMapExtractor() {
		return new MultipleSectionDictionaryLangPFCOptimizedExtractor(this);
	}

	@Override
	public boolean isMultiSectionDictionary() {
		return true;
	}

	public int getObjectsSectionCount() {
		return objectIdLocationsSec.length;
	}

	public ObjectIdLocationData getObjectsSectionFromId(int id) {
		return objectIdLocationsSec[id];
	}
}
