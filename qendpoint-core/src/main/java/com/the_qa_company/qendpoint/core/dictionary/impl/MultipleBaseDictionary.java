package com.the_qa_company.qendpoint.core.dictionary.impl;

import com.the_qa_company.qendpoint.core.dictionary.DictionaryPrivate;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilCat.SectionUtil;
import com.the_qa_company.qendpoint.core.enums.DictionarySectionRole;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.iterator.utils.CatIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.StringSuffixIterator;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.ByteStringUtil;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Stream;

public abstract class MultipleBaseDictionary implements DictionaryPrivate {
	protected final HDTOptions spec;

	protected DictionarySectionPrivate subjects;
	protected DictionarySectionPrivate predicates;
	protected TreeMap<ByteString, DictionarySectionPrivate> objects;
	protected DictionarySectionPrivate shared;

	public MultipleBaseDictionary(HDTOptions spec) {
		this.spec = spec;
	}

	protected long getGlobalId(long id, DictionarySectionRole position, CharSequence str) {
		switch (position) {
		case SUBJECT -> {
			return id + shared.getNumberOfElements();
		}
		case OBJECT -> {
			Iterator<Map.Entry<ByteString, DictionarySectionPrivate>> iter = objects.entrySet().iterator();
			int count = 0;
			ByteString type = (ByteString) LiteralsUtils.getType(ByteStringUtil.asByteString(str));
			while (iter.hasNext()) {
				Map.Entry<ByteString, DictionarySectionPrivate> entry = iter.next();
				count += entry.getValue().getNumberOfElements();
				if (type.equals(entry.getKey())) {
					count -= entry.getValue().getNumberOfElements();
					break;
				}

			}
			return shared.getNumberOfElements() + count + id;
		}
		case PREDICATE, SHARED -> {
			return id;
		}
		default -> throw new IllegalArgumentException();
		}
	}

	/*
	 * TODO: Change the objects part to look over the sections according to some
	 * pointer
	 */
	protected long getLocalId(long id, TripleComponentRole position) {
		switch (position) {
		case SUBJECT -> {
			if (id <= shared.getNumberOfElements())
				return id;
			else
				return id - shared.getNumberOfElements();
		}
		case OBJECT -> {
			if (id <= shared.getNumberOfElements()) {
				return id;
			} else {
				Iterator<Map.Entry<ByteString, DictionarySectionPrivate>> hmIterator = objects.entrySet().iterator();
				// iterate over all subsections in the objects section
				long count = 0;
				while (hmIterator.hasNext()) {
					Map.Entry<ByteString, DictionarySectionPrivate> entry = hmIterator.next();
					long numElts;

					// what???
					// if (entry.getValue() instanceof PFCOptimizedExtractor) {
					// numElts =
					// ((PFCOptimizedExtractor)entry.getValue()).getNumStrings();
					// } else {
					numElts = entry.getValue().getNumberOfElements();
					// }
					count += numElts;
					if (id <= shared.getNumberOfElements() + count) {
						count -= numElts;
						break;
					}
				}
				// subtract the number of elements in the shared + the
				// subsections in the objects section
				return id - count - shared.getNumberOfElements();
			}
		}
		case PREDICATE -> {
			return id;
		}
		default -> throw new IllegalArgumentException();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.Dictionary#stringToId(java.lang.CharSequence,
	 * datatypes.TripleComponentRole)
	 */
	@Override
	public long stringToId(CharSequence sstr, TripleComponentRole position) {
		if (sstr == null || sstr.length() == 0) {
			return 0;
		}

		ByteString str = ByteString.of(sstr);

		long ret;
		switch (position) {
		case SUBJECT -> {
			ret = shared.locate(str);
			if (ret != 0) {
				return getGlobalId(ret, DictionarySectionRole.SHARED, str);
			}
			ret = subjects.locate(str);
			if (ret != 0) {
				return getGlobalId(ret, DictionarySectionRole.SUBJECT, str);
			}
			return -1;
		}
		case PREDICATE -> {
			ret = predicates.locate(str);
			if (ret != 0) {
				return getGlobalId(ret, DictionarySectionRole.PREDICATE, str);
			}
			return -1;
		}
		case OBJECT -> {
			if (str.charAt(0) != '"') {
				ret = shared.locate(str);
				if (ret != 0) {
					return getGlobalId(ret, DictionarySectionRole.SHARED, str);
				}
			}
			DictionarySectionPrivate subSection = getSubSection(str);
			if (subSection != null) {
				ret = subSection.locate(LiteralsUtils.removeType(str));
			} else {
				return -1;
			}
			if (ret != 0) {
				return getGlobalId(ret, DictionarySectionRole.OBJECT, str);
			}
			return -1;
		}
		default -> throw new IllegalArgumentException();
		}
	}

	private long getNumberObjectsAllSections() {
		// iterate over all subsections in the objects section
		return objects.values().stream().mapToLong(DictionarySection::getNumberOfElements).sum();
	}

	@Override
	public long getNumberOfElements() {

		return subjects.getNumberOfElements() + predicates.getNumberOfElements() + getNumberObjectsAllSections()
				+ shared.getNumberOfElements();
	}

	@Override
	public long size() {
		return subjects.size() + predicates.size() + objects.values().stream().mapToLong(DictionarySection::size).sum()
				+ shared.size();
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
		return getNumberObjectsAllSections() + shared.getNumberOfElements();
	}

	@Override
	public long getNshared() {
		return shared.getNumberOfElements();
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
	public Map<? extends CharSequence, DictionarySection> getAllObjects() {
		return new TreeMap<>(this.objects);
	}

	@Override
	public DictionarySection getObjects() {
		throw new NotImplementedException();
	}

	@Override
	public DictionarySection getShared() {
		return shared;
	}

	private AbstractMap.SimpleEntry<CharSequence, DictionarySectionPrivate> getSection(long id,
			TripleComponentRole role) {
		switch (role) {
		case SUBJECT -> {
			if (id <= shared.getNumberOfElements()) {
				return new AbstractMap.SimpleEntry<>(LiteralsUtils.NO_DATATYPE, shared);
			} else {
				return new AbstractMap.SimpleEntry<>(LiteralsUtils.NO_DATATYPE, subjects);
			}
		}
		case PREDICATE -> {
			return new AbstractMap.SimpleEntry<>(LiteralsUtils.NO_DATATYPE, predicates);
		}
		case OBJECT -> {
			if (id <= shared.getNumberOfElements()) {
				return new AbstractMap.SimpleEntry<>(LiteralsUtils.NO_DATATYPE, shared);
			} else {

				Iterator<Map.Entry<ByteString, DictionarySectionPrivate>> hmIterator = objects.entrySet().iterator();
				// iterate over all subsections in the objects section
				DictionarySectionPrivate desiredSection = null;
				ByteString type = ByteString.empty();
				int count = 0;
				while (hmIterator.hasNext()) {
					Map.Entry<ByteString, DictionarySectionPrivate> entry = hmIterator.next();
					DictionarySectionPrivate subSection = entry.getValue();
					count += subSection.getNumberOfElements();
					if (id <= shared.getNumberOfElements() + count) {
						desiredSection = subSection;
						type = entry.getKey();
						break;
					}
				}
				return new AbstractMap.SimpleEntry<>(type, desiredSection);
			}
		}
		default -> throw new IllegalArgumentException();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.Dictionary#idToString(int,
	 * datatypes.TripleComponentRole)
	 */
	@Override
	public ByteString idToString(long id, TripleComponentRole role) {
		AbstractMap.SimpleEntry<CharSequence, DictionarySectionPrivate> section = getSection(id, role);
		long localId = getLocalId(id, role);
		if (section.getKey().equals(LiteralsUtils.NO_DATATYPE) || section.getKey().equals(SectionUtil.SECTION))
			return ByteString.of(section.getValue().extract(localId));
		else {
			assert section.getValue() != null : "Error couldn't find the section for the given ID: [" + id + "]";

			CharSequence label = section.getValue().extract(localId);
			CharSequence dType = section.getKey();
			// Matcher matcher = pattern.matcher(label);
			if (LiteralsUtils.containsLanguage(label)) {
				return ByteString.of(label);
			} else {
				return ByteString.of(label + "^^" + dType);
			}
		}
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
			Stream<? extends Iterator<? extends CharSequence>> os = getAllObjects().entrySet().stream().map(e -> {
				if (LiteralsUtils.NO_DATATYPE.equals(e.getKey())) {
					return e.getValue().getSortedEntries();
				}
				ByteString suffix = LiteralsUtils.TYPE_OPERATOR.copyAppend(e.getKey());
				return StringSuffixIterator.of(e.getValue().getSortedEntries(), suffix);
			});
			if (!includeShared) {
				return CatIterator.of(os.toList());
			}

			return CatIterator.of(Stream.concat(Stream.of(getShared().getSortedEntries()), os).toList());
		}
		default -> throw new IllegalArgumentException("Unknown role: " + role);
		}
	}

	private DictionarySectionPrivate getSubSection(ByteString str) {
		return objects.get((ByteString) LiteralsUtils.getType(str));
	}

	@Override
	public CharSequence dataTypeOfId(long id) {
		return getSection(id, TripleComponentRole.OBJECT).getKey();
	}

	@Override
	public boolean supportsDataTypeOfId() {
		return true;
	}

	public AbstractMap.SimpleEntry<Long, Long> getDataTypeRange(CharSequence dataType) {
		ByteString seq = LiteralsUtils.embed(ByteStringUtil.asByteString(dataType));
		if (objects.containsKey(seq)) { // literals subsection exist
			Iterator<Map.Entry<ByteString, DictionarySectionPrivate>> iter = objects.entrySet().iterator();
			int count = 0;
			while (iter.hasNext()) {
				Map.Entry<ByteString, DictionarySectionPrivate> entry = iter.next();
				count += entry.getValue().getNumberOfElements();
				if (seq.equals(entry.getKey())) {
					count -= entry.getValue().getNumberOfElements();
					break;
				}

			}
			long offset = shared.getNumberOfElements() + count;
			long size = offset + objects.get(seq).getNumberOfElements();
			return new AbstractMap.SimpleEntry<>(offset + 1, size);
		}
		return new AbstractMap.SimpleEntry<>(0L, 0L);
	}

	protected static class StopPredicate<T extends CharSequence> implements Predicate<T> {
		private CharSequence type;

		@Override
		public boolean test(T charSequence) {
			CharSequence type = LiteralsUtils.getType(charSequence);
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
}
