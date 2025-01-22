/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/
 * dictionary/DictionaryFactory.java $ Revision: $Rev: 191 $ Last modified:
 * $Date: 2013-03-03 11:41:43 +0000 (dom, 03 mar 2013) $ Last modified by:
 * $Author: mario.arias $ This library is free software; you can redistribute it
 * and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; version 3.0 of the License. This
 * library is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 * Contacting the authors: Mario Arias: mario.arias@deri.org Javier D.
 * Fernandez: jfergar@infor.uva.es Miguel A. Martinez-Prieto:
 * migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.dictionary;

import com.the_qa_company.qendpoint.core.dictionary.impl.FourQuadSectionDictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.FourSectionDictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.FourSectionDictionaryBig;
import com.the_qa_company.qendpoint.core.dictionary.impl.FourSectionDictionaryDiff;
import com.the_qa_company.qendpoint.core.dictionary.impl.HashDictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.HashQuadDictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.MultipleSectionDictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.MultipleSectionDictionaryDiff;
import com.the_qa_company.qendpoint.core.dictionary.impl.MultipleSectionDictionaryLang;
import com.the_qa_company.qendpoint.core.dictionary.impl.MultipleSectionDictionaryLangPrefixes;
import com.the_qa_company.qendpoint.core.dictionary.impl.PSFCFourSectionDictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.PSFCTempDictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.WriteFourSectionDictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.WriteMultipleSectionDictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.WriteMultipleSectionDictionaryLang;
import com.the_qa_company.qendpoint.core.dictionary.impl.WriteMultipleSectionDictionaryLangPrefixes;
import com.the_qa_company.qendpoint.core.dictionary.impl.kcat.FourSectionDictionaryKCat;
import com.the_qa_company.qendpoint.core.dictionary.impl.kcat.MultipleSectionDictionaryKCat;
import com.the_qa_company.qendpoint.core.dictionary.impl.kcat.MultipleSectionLangDictionaryKCat;
import com.the_qa_company.qendpoint.core.dictionary.impl.kcat.MultipleSectionLangPrefixesDictionaryKCat;
import com.the_qa_company.qendpoint.core.enums.CompressionType;
import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.MultiSectionLangPrefixSectionCompressor;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.MultiSectionLangSectionCompressor;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.MultiSectionSectionCompressor;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.SectionCompressor;
import com.the_qa_company.qendpoint.core.iterator.utils.AsyncIteratorFetcher;
import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;
import com.the_qa_company.qendpoint.core.options.ControlInfo;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.PrefixesStorage;

import java.nio.file.Path;
import java.util.TreeMap;

/**
 * Factory that creates Dictionary objects
 */
public class DictionaryFactory {

	/**
	 * @deprecated use {@link HDTOptionsKeys#TEMP_DICTIONARY_IMPL_VALUE_HASH}
	 *             instead
	 */
	@Deprecated
	public static final String MOD_DICT_IMPL_HASH = HDTOptionsKeys.TEMP_DICTIONARY_IMPL_VALUE_HASH;
	/**
	 * @deprecated use
	 *             {@link HDTOptionsKeys#TEMP_DICTIONARY_IMPL_VALUE_MULT_HASH}
	 *             instead
	 */
	@Deprecated
	public static final String MOD_DICT_IMPL_MULT_HASH = HDTOptionsKeys.TEMP_DICTIONARY_IMPL_VALUE_MULT_HASH;
	/**
	 * @deprecated use
	 *             {@link HDTOptionsKeys#TEMP_DICTIONARY_IMPL_VALUE_HASH_PSFC}
	 *             instead
	 */
	@Deprecated
	public static final String MOD_DICT_IMPL_HASH_PSFC = HDTOptionsKeys.TEMP_DICTIONARY_IMPL_VALUE_HASH_PSFC;
	/**
	 * @deprecated use
	 *             {@link HDTOptionsKeys#DICTIONARY_TYPE_VALUE_FOUR_SECTION_BIG}
	 *             instead
	 */
	@Deprecated
	public static final String DICTIONARY_TYPE_FOUR_SECTION_BIG = HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_SECTION_BIG;
	/**
	 * @deprecated use
	 *             {@link HDTOptionsKeys#DICTIONARY_TYPE_VALUE_MULTI_OBJECTS}
	 *             instead
	 */
	@Deprecated
	public static final String DICTIONARY_TYPE_MULTI_OBJECTS = HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS;

	private DictionaryFactory() {
	}

	public static boolean isQuadDictionary(String name) {
		return switch (name) {
		case HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_QUAD_SECTION,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG_QUAD,
				HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG_QUAD ->
			true;
		default -> false;
		};
	}

	/**
	 * Creates a temp dictionary (allow insert)
	 *
	 * @param spec specs to read dictionary
	 * @return TempDictionary
	 */
	public static TempDictionary createTempDictionary(HDTOptions spec) {
		String name = spec.get(HDTOptionsKeys.TEMP_DICTIONARY_IMPL_KEY, "");

		// Implementations available in the Core
		return switch (name) {
		case "" -> {
			String dicttype = spec.get(HDTOptionsKeys.DICTIONARY_TYPE_KEY, "");
			if (dicttype.equals(HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_PSFC_SECTION)) {
				yield new PSFCTempDictionary(new HashDictionary(spec));
			}
			if (isQuadDictionary(dicttype)) {
				yield new HashQuadDictionary(spec);
			}
			yield new HashDictionary(spec);
		}
		case HDTOptionsKeys.TEMP_DICTIONARY_IMPL_VALUE_HASH, HDTOptionsKeys.TEMP_DICTIONARY_IMPL_VALUE_MULT_HASH ->
			new HashDictionary(spec);
		case HDTOptionsKeys.TEMP_DICTIONARY_IMPL_VALUE_HASH_QUAD -> new HashQuadDictionary(spec);
		case HDTOptionsKeys.TEMP_DICTIONARY_IMPL_VALUE_HASH_PSFC -> new PSFCTempDictionary(new HashDictionary(spec));
		default -> throw new IllegalFormatException("Implementation of triples not found for " + name);
		};
	}

	/**
	 * Creates a dictionary
	 *
	 * @param spec specs to read dictionary
	 * @return Dictionary
	 */
	public static DictionaryPrivate createDictionary(HDTOptions spec) {
		String name = spec.get(HDTOptionsKeys.DICTIONARY_TYPE_KEY, "");
		return switch (name) {
		case "", HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_SECTION -> new FourSectionDictionary(spec);
		case HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_QUAD_SECTION -> new FourQuadSectionDictionary(spec);
		case HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_PSFC_SECTION -> new PSFCFourSectionDictionary(spec);
		case HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_SECTION_BIG -> new FourSectionDictionaryBig(spec);
		case HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS -> new MultipleSectionDictionary(spec);
		case HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG -> new MultipleSectionDictionaryLang(spec);
		case HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG_QUAD ->
			new MultipleSectionDictionaryLang(spec, true);
		case HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG_PREFIXES ->
			new MultipleSectionDictionaryLangPrefixes(spec);
		default -> throw new IllegalFormatException("Implementation of dictionary not found for " + name);
		};
	}

	/**
	 * Creates a write-dictionary
	 *
	 * @param spec       specs to read dictionary
	 * @param location   write location
	 * @param bufferSize write buffer sizes
	 * @return WriteDictionary
	 */
	public static DictionaryPrivate createWriteDictionary(HDTOptions spec, Path location, int bufferSize) {
		String name = spec.get(HDTOptionsKeys.DICTIONARY_TYPE_KEY, "");
		return createWriteDictionary(name, spec, location, bufferSize);
	}

	/**
	 * Creates a write-dictionary
	 *
	 * @param name       name of the HDT Dictionary type
	 * @param spec       specs to read dictionary
	 * @param location   write location
	 * @param bufferSize write buffer sizes
	 * @return WriteDictionary
	 */
	public static DictionaryPrivate createWriteDictionary(String name, HDTOptions spec, Path location, int bufferSize) {
		return switch (name) {
		case "", HDTVocabulary.DICTIONARY_TYPE_FOUR_SECTION, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_SECTION_BIG ->
			new WriteFourSectionDictionary(spec, location, bufferSize, false);
		case HDTVocabulary.DICTIONARY_TYPE_FOUR_QUAD_SECTION ->
			new WriteFourSectionDictionary(spec, location, bufferSize, true);
		case HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS ->
			new WriteMultipleSectionDictionary(spec, location, bufferSize);
		case HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG ->
			new WriteMultipleSectionDictionaryLang(spec, location, bufferSize);
		case HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG_PREFIXES,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG_PREFIXES ->
			new WriteMultipleSectionDictionaryLangPrefixes(spec, location, bufferSize);
		case HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG_QUAD,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG_QUAD ->
			new WriteMultipleSectionDictionaryLang(spec, location, bufferSize, true);
		default -> throw new IllegalFormatException("Implementation of write dictionary not found for " + name);
		};
	}

	public static SectionCompressor createSectionCompressor(HDTOptions spec, CloseSuppressPath baseFileName,
			AsyncIteratorFetcher<TripleString> source, MultiThreadListener listener, int bufferSize, long chunkSize,
			int k, boolean debugSleepKwayDict, CompressionType compressionType) {
		String name = spec.get(HDTOptionsKeys.DICTIONARY_TYPE_KEY, "");

		// use the same compressor for quad/triple dict types
		boolean quad = isQuadDictionary(name);

		return switch (name) {
		case "", HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_SECTION,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_SECTION_BIG,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_QUAD_SECTION ->
			new SectionCompressor(baseFileName, source, listener, bufferSize, chunkSize, k, debugSleepKwayDict, quad,
					compressionType);
		case HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS -> new MultiSectionSectionCompressor(baseFileName,
				source, listener, bufferSize, chunkSize, k, debugSleepKwayDict, quad, compressionType);
		case HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG_QUAD ->
			new MultiSectionLangSectionCompressor(baseFileName, source, listener, bufferSize, chunkSize, k,
					debugSleepKwayDict, quad, compressionType);
		case HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG_PREFIXES ->
			new MultiSectionLangPrefixSectionCompressor(baseFileName, source, listener, bufferSize, chunkSize, k,
					debugSleepKwayDict, quad, spec, compressionType);
		default -> throw new IllegalFormatException("Implementation of section compressor not found for " + name);
		};
	}

	/**
	 * Creates a dictionary
	 *
	 * @param ci specs to read dictionary
	 * @return Dictionary
	 */
	public static DictionaryPrivate createDictionary(ControlInfo ci) {
		String name = ci.getFormat();
		return switch (name) {
		case HDTVocabulary.DICTIONARY_TYPE_FOUR_SECTION -> new FourSectionDictionary(HDTOptions.of());
		case HDTVocabulary.DICTIONARY_TYPE_FOUR_QUAD_SECTION -> new FourQuadSectionDictionary(HDTOptions.of());
		case HDTVocabulary.DICTIONARY_TYPE_FOUR_PSFC_SECTION -> new PSFCFourSectionDictionary(HDTOptions.of());
		case HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION -> new MultipleSectionDictionary(HDTOptions.of());
		case HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG -> new MultipleSectionDictionaryLang(HDTOptions.of());
		case HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG_QUAD ->
			new MultipleSectionDictionaryLang(HDTOptions.of(), true);
		case HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG_PREFIXES ->
			new MultipleSectionDictionaryLangPrefixes(HDTOptions.of());
		default -> throw new IllegalFormatException("Implementation of dictionary not found for " + name);
		};
	}

	/**
	 * create a {@link DictionaryDiff} to create diff of a HDT in a new location
	 *
	 * @param dictionary the hdt dictionary
	 * @param location   the location of the new dictionary
	 * @return dictionaryDiff
	 */
	public static DictionaryDiff createDictionaryDiff(Dictionary dictionary, String location) {
		String type = dictionary.getType();
		return switch (type) {
		case HDTVocabulary.DICTIONARY_TYPE_FOUR_SECTION, HDTVocabulary.DICTIONARY_TYPE_FOUR_PSFC_SECTION ->
			new FourSectionDictionaryDiff(location);
		case HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION -> new MultipleSectionDictionaryDiff(location);
		default -> throw new IllegalFormatException("Implementation of DictionaryDiff not found for " + type);
		};
	}

	/**
	 * create {@link DictionaryKCat} for HDTCat
	 *
	 * @param dictionary dictionary
	 * @return dictionaryKCat
	 */
	public static DictionaryKCat createDictionaryKCat(Dictionary dictionary) {
		String type = dictionary.getType();
		return switch (type) {
		case HDTVocabulary.DICTIONARY_TYPE_FOUR_SECTION, HDTVocabulary.DICTIONARY_TYPE_FOUR_QUAD_SECTION ->
			new FourSectionDictionaryKCat(dictionary);
		case HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION -> new MultipleSectionDictionaryKCat(dictionary);
		case HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG, HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG_QUAD ->
			new MultipleSectionLangDictionaryKCat(dictionary);
		case HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG_PREFIXES ->
			new MultipleSectionLangPrefixesDictionaryKCat(dictionary);
		default -> throw new IllegalArgumentException("Implementation of DictionaryKCat not found for " + type);
		};
	}

	public static DictionaryPrivate createWriteDictionary(String type, HDTOptions spec,
			DictionarySectionPrivate subject, DictionarySectionPrivate predicate, DictionarySectionPrivate object,
			DictionarySectionPrivate shared, TreeMap<ByteString, DictionarySectionPrivate> sub,
			DictionarySectionPrivate graph, PrefixesStorage prefixesStorage) {
		if (graph == null) {
			return switch (type) {
			case HDTVocabulary.DICTIONARY_TYPE_FOUR_SECTION, HDTVocabulary.DICTIONARY_TYPE_FOUR_PSFC_SECTION ->
				new WriteFourSectionDictionary(spec, subject, predicate, object, shared);
			case HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION ->
				new WriteMultipleSectionDictionary(spec, subject, predicate, shared, sub);
			case HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG ->
				new WriteMultipleSectionDictionaryLang(spec, subject, predicate, shared, sub);
			case HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG_PREFIXES ->
				new WriteMultipleSectionDictionaryLangPrefixes(spec, subject, predicate, shared, sub, prefixesStorage);
			default -> throw new IllegalArgumentException("Unknown dictionary type " + type);
			};
		}
		return switch (type) {
		case HDTVocabulary.DICTIONARY_TYPE_FOUR_QUAD_SECTION ->
			new WriteFourSectionDictionary(spec, subject, predicate, object, shared, graph);
		case HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG_QUAD ->
			new WriteMultipleSectionDictionaryLang(spec, subject, predicate, shared, sub, graph);
		default -> throw new IllegalArgumentException("Unknown quad dictionary type " + type);
		};
	}
}
