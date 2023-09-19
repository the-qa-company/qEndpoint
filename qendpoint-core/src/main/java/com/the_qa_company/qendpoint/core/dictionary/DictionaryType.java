package com.the_qa_company.qendpoint.core.dictionary;

import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.util.string.CharSequenceComparator;
import com.the_qa_company.qendpoint.core.util.string.CharSequenceDTComparator;
import com.the_qa_company.qendpoint.core.util.string.CharSequenceDTLComparator;

import java.util.Comparator;
import java.util.Objects;

public enum DictionaryType {
	/**
	 * Four section dict generation
	 */
	FSD(false, false, CharSequenceComparator.getInstance()),
	/**
	 * Multi sections dict generation
	 */
	MSD(true, false, CharSequenceDTComparator.getInstance()),
	/**
	 * Multi sections lang dict generation
	 */
	MSDL(true, true, CharSequenceDTLComparator.getInstance());

	public static DictionaryType fromDictionaryType(HDTOptions options) {
		return fromDictionaryType(HDTOptions.ofNullable(options).get(HDTOptionsKeys.DICTIONARY_TYPE_KEY, ""));
	}

	public static DictionaryType fromDictionaryType(String dictType) {
		return switch (Objects.requireNonNullElse(dictType, "")) {
		case "", HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_SECTION,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_QUAD_SECTION,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_SECTION_BIG,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_PSFC_SECTION ->
			FSD;
		case HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS, HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION -> MSD;
			case HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG,
					HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG_QUAD ->
			MSDL;
		default -> throw new NotImplementedException("Can't find type for name: " + dictType);
		};
	}

	private final boolean countTypes;
	private final boolean countLangs;
	private final Comparator<CharSequence> comparator;

	DictionaryType(boolean countTypes, boolean countLangs, Comparator<CharSequence> comparator) {
		this.countTypes = countTypes;
		this.countLangs = countLangs;
		this.comparator = comparator;
	}

	public boolean countTypes() {
		return countTypes;
	}

	public boolean countLangs() {
		return countLangs;
	}

	public Comparator<CharSequence> comparator() {
		return comparator;
	}
}
