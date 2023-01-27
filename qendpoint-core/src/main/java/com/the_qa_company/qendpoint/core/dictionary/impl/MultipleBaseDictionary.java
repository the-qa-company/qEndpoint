package com.the_qa_company.qendpoint.core.dictionary.impl;

import com.the_qa_company.qendpoint.core.dictionary.DictionaryPrivate;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilCat.SectionUtil;
import com.the_qa_company.qendpoint.core.enums.DictionarySectionRole;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.ByteStringUtil;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

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
		case SUBJECT:
			return id + shared.getNumberOfElements();
		case OBJECT: {
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

		case PREDICATE:
		case SHARED:
			return id;
		default:
			throw new IllegalArgumentException();
		}
	}

	/*
	 * TODO: Change the objects part to look over the sections according to some
	 * pointer
	 */
	protected long getLocalId(long id, TripleComponentRole position) {
		switch (position) {
		case SUBJECT:
			if (id <= shared.getNumberOfElements())
				return id;
			else
				return id - shared.getNumberOfElements();
		case OBJECT:
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
		case PREDICATE:
			return id;
		default:
			throw new IllegalArgumentException();
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
		case SUBJECT:
			ret = shared.locate(str);
			if (ret != 0) {
				return getGlobalId(ret, DictionarySectionRole.SHARED, str);
			}
			ret = subjects.locate(str);
			if (ret != 0) {
				return getGlobalId(ret, DictionarySectionRole.SUBJECT, str);
			}
			return -1;
		case PREDICATE:
			ret = predicates.locate(str);
			if (ret != 0) {
				return getGlobalId(ret, DictionarySectionRole.PREDICATE, str);
			}
			return -1;
		case OBJECT:
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
		default:
			throw new IllegalArgumentException();
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
		case SUBJECT:
			if (id <= shared.getNumberOfElements()) {
				return new AbstractMap.SimpleEntry<>(SectionUtil.SECTION, shared);
			} else {
				return new AbstractMap.SimpleEntry<>(SectionUtil.SECTION, subjects);
			}
		case PREDICATE:
			return new AbstractMap.SimpleEntry<>(SectionUtil.SECTION, predicates);
		case OBJECT:
			if (id <= shared.getNumberOfElements()) {
				return new AbstractMap.SimpleEntry<>(SectionUtil.SECTION, shared);
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
		default:
			throw new IllegalArgumentException();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.Dictionary#idToString(int,
	 * datatypes.TripleComponentRole)
	 */
	@Override
	public CharSequence idToString(long id, TripleComponentRole role) {
		AbstractMap.SimpleEntry<CharSequence, DictionarySectionPrivate> section = getSection(id, role);
		long localId = getLocalId(id, role);
		if (section.getKey().equals(LiteralsUtils.NO_DATATYPE) || section.getKey().equals(SectionUtil.SECTION))
			return section.getValue().extract(localId);
		else {
			if (section.getValue() == null) {
				// this should not happen, means that the given id wasn't found
				// in any section
				System.out.println("Error couldn't find the section for the given ID: [" + id + "]");
				return null;
			} else {
				CharSequence label = section.getValue().extract(localId);
				CharSequence dType = section.getKey();
				// Matcher matcher = pattern.matcher(label);
				if (LiteralsUtils.containsLanguage(label)) {
					return label;
				} else {
					return label + "^^" + dType;
				}
			}
		}
	}

	private DictionarySectionPrivate getSubSection(ByteString str) {
		return objects.get((ByteString) LiteralsUtils.getType(str));
	}

	@Override
	public CharSequence dataTypeOfId(long id) {
		return getSection(id, TripleComponentRole.OBJECT).getKey();
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
}
