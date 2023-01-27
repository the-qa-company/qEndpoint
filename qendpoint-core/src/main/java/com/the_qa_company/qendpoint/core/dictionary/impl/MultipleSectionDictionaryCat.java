/**
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; version 3.0 of the License. This library is distributed
 * in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * library; if not, write to the Free Software Foundation, Inc., 51 Franklin St,
 * Fifth Floor, Boston, MA 02110-1301 USA Contacting the authors: Dennis
 * Diefenbach: dennis.diefenbach@univ-st-etienne.fr Jose Gimenez Garcia:
 * jose.gimenez.garcia@univ-st-etienne.fr
 */

package com.the_qa_company.qendpoint.core.dictionary.impl;

import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.dictionary.DictionaryCat;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilCat.CatElement;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilCat.CatIntersection;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilCat.CatMapping;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilCat.CatMappingBack;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilCat.CatUnion;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilCat.CatWrapper;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilCat.SectionUtil;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.ControlInfo;
import com.the_qa_company.qendpoint.core.options.ControlInformation;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64BigDisk;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.listener.ListenerUtil;
import com.the_qa_company.qendpoint.core.util.listener.PrefixListener;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.ByteStringUtil;
import com.the_qa_company.qendpoint.core.util.string.CharSequenceComparator;
import com.the_qa_company.qendpoint.core.util.string.CompactString;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MultipleSectionDictionaryCat implements DictionaryCat {

	private static final int DEFAULT_BLOCK_SIZE = 16;
	private static final int BLOCK_PER_BUFFER = 1000000;
	private static final ByteString NO_DT_OBJECTS = LiteralsUtils.NO_DATATYPE;
	private static final ByteString NO_DT_OBJECTS_1 = NO_DT_OBJECTS.copyAppend("1");
	private static final ByteString NO_DT_OBJECTS_2 = NO_DT_OBJECTS.copyAppend("2");
	private final String location;
	private long numShared;

	private final HashMap<ByteString, CatMapping> allMappings = new HashMap<>();

	private CatMappingBack mappingS;

	public MultipleSectionDictionaryCat(String location) {
		this.location = location;
	}

	public void cat(Dictionary dictionary1, Dictionary dictionary2, ProgressListener listener) throws IOException {
		Comparator<CharSequence> comparator = CharSequenceComparator.getInstance();
		// Initialize all mappings ......

		allMappings.put(SectionUtil.P1,
				new CatMapping(location, SectionUtil.P1, dictionary1.getPredicates().getNumberOfElements()));
		allMappings.put(SectionUtil.P2,
				new CatMapping(location, SectionUtil.P2, dictionary2.getPredicates().getNumberOfElements()));
		allMappings.put(SectionUtil.S1,
				new CatMapping(location, SectionUtil.S1, dictionary1.getSubjects().getNumberOfElements()));
		allMappings.put(SectionUtil.S2,
				new CatMapping(location, SectionUtil.S2, dictionary2.getSubjects().getNumberOfElements()));
		allMappings.put(SectionUtil.O1, new CatMapping(location, SectionUtil.O1, dictionary1.getNAllObjects()));
		allMappings.put(SectionUtil.O2, new CatMapping(location, SectionUtil.O2, dictionary2.getNAllObjects()));
		allMappings.put(SectionUtil.SH1,
				new CatMapping(location, SectionUtil.SH1, dictionary1.getShared().getNumberOfElements()));
		allMappings.put(SectionUtil.SH2,
				new CatMapping(location, SectionUtil.SH2, dictionary2.getShared().getNumberOfElements()));
		Map<? extends CharSequence, DictionarySection> allObjects1 = dictionary1.getAllObjects();
		Iterator<? extends Map.Entry<? extends CharSequence, DictionarySection>> hmIterator1 = allObjects1.entrySet()
				.iterator();
		int countSubSections1 = 0;
		int countSubSections2 = 0;

		while (hmIterator1.hasNext()) {
			Map.Entry<? extends CharSequence, DictionarySection> entry = hmIterator1.next();
			ByteString prefix;
			if ((entry.getKey()).equals(NO_DT_OBJECTS)) {
				prefix = NO_DT_OBJECTS;
			} else {
				prefix = SectionUtil.createSub(countSubSections1);
			}
			prefix = prefix.copyAppend("1");
			allMappings.put(prefix, new CatMapping(location, prefix, entry.getValue().getNumberOfElements()));
			countSubSections1++;
		}
		Map<? extends CharSequence, DictionarySection> allObjects2 = dictionary2.getAllObjects();
		Iterator<? extends Map.Entry<? extends CharSequence, DictionarySection>> hmIterator2 = allObjects2.entrySet()
				.iterator();
		while (hmIterator2.hasNext()) {
			Map.Entry<? extends CharSequence, DictionarySection> entry = hmIterator2.next();
			ByteString prefix;
			if ((entry.getKey()).equals(NO_DT_OBJECTS)) {
				prefix = NO_DT_OBJECTS;
			} else {
				prefix = SectionUtil.createSub(countSubSections2);
			}
			prefix = prefix.copyAppend("2");
			allMappings.put(prefix, new CatMapping(location, prefix, entry.getValue().getNumberOfElements()));
			countSubSections2++;
		}

		ProgressListener iListener;

//        System.out.println("PREDICATES-------------------");
		iListener = PrefixListener.of("Generate predicates: ", listener);
		if (iListener != null) {
			iListener.notifyProgress(0, "start");
		}

		int numCommonPredicates = 0;
		CatIntersection commonP1P2 = new CatIntersection(
				new CatWrapper(dictionary1.getPredicates().getSortedEntries(), SectionUtil.P1),
				new CatWrapper(dictionary2.getPredicates().getSortedEntries(), SectionUtil.P2));
		while (commonP1P2.hasNext()) {
			commonP1P2.next();
			numCommonPredicates++;
		}
		long numPredicates = dictionary1.getPredicates().getNumberOfElements()
				+ dictionary2.getPredicates().getNumberOfElements() - numCommonPredicates;

		List<Iterator<CatElement>> addPredicatesList = new ArrayList<>();
		addPredicatesList.add(new CatWrapper(dictionary1.getPredicates().getSortedEntries(), SectionUtil.P1));
		addPredicatesList.add(new CatWrapper(dictionary2.getPredicates().getSortedEntries(), SectionUtil.P2));
		CatUnion itAddPredicates = new CatUnion(addPredicatesList);
		catSection(numPredicates, 3, itAddPredicates, new CatUnion(new ArrayList<>()), allMappings, iListener);

//        System.out.println("SUBJECTS-------------------");
		iListener = PrefixListener.of("Generate subjects: ", listener);
		if (iListener != null) {
			iListener.notifyProgress(0, "start");
		}

		ArrayList<Iterator<CatElement>> skipSubjectList = new ArrayList<>();

		skipSubjectList
				.add(new CatIntersection(new CatWrapper(dictionary1.getSubjects().getSortedEntries(), SectionUtil.S1),
						new CatWrapper(dictionary2.getShared().getSortedEntries(), SectionUtil.SH2)));
		if (allObjects2.containsKey(NO_DT_OBJECTS)) {
			skipSubjectList.add(
					new CatIntersection(new CatWrapper(dictionary1.getSubjects().getSortedEntries(), SectionUtil.S1),
							new CatWrapper(allObjects2.get(NO_DT_OBJECTS).getSortedEntries(), NO_DT_OBJECTS_2)));
		}
		skipSubjectList
				.add(new CatIntersection(new CatWrapper(dictionary2.getSubjects().getSortedEntries(), SectionUtil.S2),
						new CatWrapper(dictionary1.getShared().getSortedEntries(), SectionUtil.SH1)));
		if (allObjects1.containsKey(NO_DT_OBJECTS)) {
			skipSubjectList.add(
					new CatIntersection(new CatWrapper(dictionary2.getSubjects().getSortedEntries(), SectionUtil.S2),
							new CatWrapper(allObjects1.get(NO_DT_OBJECTS).getSortedEntries(), NO_DT_OBJECTS_1)));
		}
		CatUnion skipSubject = new CatUnion(skipSubjectList);
		int numSkipSubjects = 0;
		while (skipSubject.hasNext()) {
			skipSubject.next();
			numSkipSubjects++;
		}
		int numCommonSubjects = 0;
		CatIntersection commonS1S2 = new CatIntersection(
				new CatWrapper(dictionary1.getSubjects().getSortedEntries(), SectionUtil.S1),
				new CatWrapper(dictionary2.getSubjects().getSortedEntries(), SectionUtil.S2));
		while (commonS1S2.hasNext()) {
			commonS1S2.next();
			numCommonSubjects++;
		}
		long numSubjects = dictionary1.getSubjects().getNumberOfElements()
				+ dictionary2.getSubjects().getNumberOfElements() - numCommonSubjects - numSkipSubjects;

		skipSubjectList = new ArrayList<>();

		skipSubjectList
				.add(new CatIntersection(new CatWrapper(dictionary1.getSubjects().getSortedEntries(), SectionUtil.S1),
						new CatWrapper(dictionary2.getShared().getSortedEntries(), SectionUtil.SH2)));
		if (allObjects2.containsKey(NO_DT_OBJECTS))
			skipSubjectList.add(
					new CatIntersection(new CatWrapper(dictionary1.getSubjects().getSortedEntries(), SectionUtil.S1),
							new CatWrapper(allObjects2.get(NO_DT_OBJECTS).getSortedEntries(), NO_DT_OBJECTS_2)));
		skipSubjectList
				.add(new CatIntersection(new CatWrapper(dictionary2.getSubjects().getSortedEntries(), SectionUtil.S2),
						new CatWrapper(dictionary1.getShared().getSortedEntries(), SectionUtil.SH1)));
		if (allObjects1.containsKey(NO_DT_OBJECTS))
			skipSubjectList.add(
					new CatIntersection(new CatWrapper(dictionary2.getSubjects().getSortedEntries(), SectionUtil.S2),
							new CatWrapper(allObjects1.get(NO_DT_OBJECTS).getSortedEntries(), NO_DT_OBJECTS_1)));
		skipSubject = new CatUnion(skipSubjectList);

		ArrayList<Iterator<CatElement>> addSubjectsList = new ArrayList<>();
		addSubjectsList.add(new CatWrapper(dictionary1.getSubjects().getSortedEntries(), SectionUtil.S1));
		addSubjectsList.add(new CatWrapper(dictionary2.getSubjects().getSortedEntries(), SectionUtil.S2));
		CatUnion itAddSubjects = new CatUnion(addSubjectsList);

		catSection(numSubjects, 2, itAddSubjects, skipSubject, allMappings, iListener);

//        System.out.println("OBJECTS-------------------");
		iListener = PrefixListener.of("Generate objects: ", listener);
		if (iListener != null) {
			iListener.notifyProgress(0, "start");
		}

		ArrayList<Iterator<CatElement>> skipObjectsList = new ArrayList<>();
		if (allObjects1.containsKey(NO_DT_OBJECTS)) {
			skipObjectsList.add(new CatIntersection(
					new CatWrapper(allObjects1.get(NO_DT_OBJECTS).getSortedEntries(), NO_DT_OBJECTS_1),
					new CatWrapper(dictionary2.getShared().getSortedEntries(), SectionUtil.SH2)));
			skipObjectsList.add(new CatIntersection(
					new CatWrapper(allObjects1.get(NO_DT_OBJECTS).getSortedEntries(), NO_DT_OBJECTS_1),
					new CatWrapper(dictionary2.getSubjects().getSortedEntries(), SectionUtil.S2)));
		}
		if (allObjects2.containsKey(NO_DT_OBJECTS)) {
			skipObjectsList.add(new CatIntersection(
					new CatWrapper(allObjects2.get(NO_DT_OBJECTS).getSortedEntries(), NO_DT_OBJECTS_2),
					new CatWrapper(dictionary1.getShared().getSortedEntries(), SectionUtil.SH1)));
			skipObjectsList.add(new CatIntersection(
					new CatWrapper(allObjects2.get(NO_DT_OBJECTS).getSortedEntries(), NO_DT_OBJECTS_2),
					new CatWrapper(dictionary1.getSubjects().getSortedEntries(), SectionUtil.S1)));
		}
		CatUnion skipObject = new CatUnion(skipObjectsList);
		int numSkipObjects = 0;
		while (skipObject.hasNext()) {
			skipObject.next();
			numSkipObjects++;
		}

		int numCommonObjects = 0;
		ArrayList<Iterator<CatElement>> commonObjectsList = new ArrayList<>();
		hmIterator1 = allObjects1.entrySet().iterator();
		hmIterator2 = allObjects2.entrySet().iterator();
		boolean skip1 = false;
		boolean skip2 = false;
		ByteString dataType1 = CompactString.EMPTY;
		ByteString dataType2 = CompactString.EMPTY;
		DictionarySection section1 = null;
		DictionarySection section2 = null;
		while (hmIterator1.hasNext() || hmIterator2.hasNext()) {

			if (hmIterator1.hasNext()) {
				if (!skip1) {
					Map.Entry<? extends CharSequence, DictionarySection> entry1 = hmIterator1.next();
					section1 = entry1.getValue();
					dataType1 = ByteString.of(entry1.getKey());
				}
			}
			if (hmIterator2.hasNext()) {
				if (!skip2) {
					Map.Entry<? extends CharSequence, DictionarySection> entry2 = hmIterator2.next();
					section2 = entry2.getValue();
					dataType2 = ByteString.of(entry2.getKey());
				}
			}
			if (section1 != null && section2 != null && dataType1.equals(dataType2)) {
				commonObjectsList.add(
						new CatIntersection(new CatWrapper(section1.getSortedEntries(), dataType1.copyAppend("_1")),
								new CatWrapper(section2.getSortedEntries(), dataType2.copyAppend("_2"))));
			} else {
				int comp = comparator.compare(dataType1, dataType2);
				if (comp > 0) {
					skip1 = true;
					skip2 = false;
				} else if (comp < 0) {
					skip1 = false;
					skip2 = true;
				}
				if (!hmIterator2.hasNext()) {
					skip1 = false;
				}
				if (!hmIterator1.hasNext()) {
					skip2 = false;
				}
			}
		}
		CatUnion commonO1O2 = new CatUnion(commonObjectsList);
		while (commonO1O2.hasNext()) {
			commonO1O2.next();
			numCommonObjects++;
		}

		skipObjectsList = new ArrayList<>();
		if (allObjects1.containsKey(NO_DT_OBJECTS)) {
			skipObjectsList.add(new CatIntersection(
					new CatWrapper(allObjects1.get(NO_DT_OBJECTS).getSortedEntries(), NO_DT_OBJECTS_1),
					new CatWrapper(dictionary2.getShared().getSortedEntries(), SectionUtil.SH2)));
			skipObjectsList.add(new CatIntersection(
					new CatWrapper(allObjects1.get(NO_DT_OBJECTS).getSortedEntries(), NO_DT_OBJECTS_1),
					new CatWrapper(dictionary2.getSubjects().getSortedEntries(), SectionUtil.S2)));
		}
		if (allObjects2.containsKey(NO_DT_OBJECTS)) {
			skipObjectsList.add(new CatIntersection(
					new CatWrapper(allObjects2.get(NO_DT_OBJECTS).getSortedEntries(), NO_DT_OBJECTS_2),
					new CatWrapper(dictionary1.getShared().getSortedEntries(), SectionUtil.SH1)));
			skipObjectsList.add(new CatIntersection(
					new CatWrapper(allObjects2.get(NO_DT_OBJECTS).getSortedEntries(), NO_DT_OBJECTS_2),
					new CatWrapper(dictionary1.getSubjects().getSortedEntries(), SectionUtil.S1)));
		}
		skipObject = new CatUnion(skipObjectsList);

		long numObject = dictionary1.getNAllObjects() + dictionary2.getNAllObjects() - numCommonObjects
				- numSkipObjects;

		hmIterator1 = allObjects1.entrySet().iterator();
		hmIterator2 = allObjects2.entrySet().iterator();
		int type = 4;
		List<ByteString> dataTypes = new ArrayList<>();
		// iterate over objects subsections and cat them together
		countSubSections1 = 0;
		countSubSections2 = 0;

		Map<ByteString, Long> offsets = new HashMap<>();
		long total = 0;
		skip1 = false;
		skip2 = false;
		dataType1 = ByteString.empty();
		dataType2 = ByteString.empty();
		section1 = null;
		section2 = null;
		ByteString prefix1 = ByteString.empty();
		ByteString prefix2 = ByteString.empty();

		while (hmIterator1.hasNext() || hmIterator2.hasNext()) {
			List<Iterator<CatElement>> addObjectsList = new ArrayList<>();
			List<Iterator<CatElement>> countObjectsList = new ArrayList<>();
			if (hmIterator1.hasNext()) {
				if (!skip1) {
					Map.Entry<? extends CharSequence, DictionarySection> entry = hmIterator1.next();
					dataType1 = ByteString.of(entry.getKey());
					section1 = entry.getValue();
					if (dataType1.equals(NO_DT_OBJECTS)) {
						prefix1 = NO_DT_OBJECTS;
					} else {
						prefix1 = SectionUtil.createSub(countSubSections1);
					}
					countSubSections1++;
				}
			}
			if (hmIterator2.hasNext()) {
				if (!skip2) {
					Map.Entry<? extends CharSequence, DictionarySection> entry = hmIterator2.next();
					dataType2 = ByteString.of(entry.getKey());
					section2 = entry.getValue();
					if (dataType2.equals(NO_DT_OBJECTS)) {
						prefix2 = NO_DT_OBJECTS;
					} else {
						prefix2 = SectionUtil.createSub(countSubSections2);
					}
					countSubSections2++;
				}
			}
			ByteString dataType = CompactString.EMPTY;
			if (section1 != null && section2 != null && dataType1.equals(dataType2)) {
				dataType = dataType1;
				addObjectsList.add(new CatWrapper(section1.getSortedEntries(), prefix1.copyAppend("1")));
				countObjectsList.add(new CatWrapper(section1.getSortedEntries(), prefix1.copyAppend("1")));
				addObjectsList.add(new CatWrapper(section2.getSortedEntries(), prefix2.copyAppend("2")));
				countObjectsList.add(new CatWrapper(section2.getSortedEntries(), prefix2.copyAppend("2")));
				skip1 = false;
				skip2 = false;
				if (!hmIterator1.hasNext()) {
					section1 = null;
					dataType1 = ByteString.empty();
				} else if (!hmIterator2.hasNext()) {
					section2 = null;
					dataType2 = ByteString.empty();
				}
			} else {
				boolean fromOne = false;
				boolean fromTwo = false;
				if (dataType1.length() == 0) {
					fromTwo = true;
				} else if (dataType2.length() == 0) {
					fromOne = true;
				}
				int comp = comparator.compare(dataType1, dataType2);
				if (comp < 0) {
					fromOne = true;
				}
				if (comp > 0) {
					fromTwo = true;
				}
				if (section1 != null && fromOne) { // section 1 before section 2
					dataType = dataType1;
					addObjectsList.add(new CatWrapper(section1.getSortedEntries(), prefix1.copyAppend("1")));
					countObjectsList.add(new CatWrapper(section1.getSortedEntries(), prefix1.copyAppend("1")));
					if (!hmIterator1.hasNext()) {
						section1 = null;
						dataType1 = ByteString.empty();
						skip2 = false;
					} else {
						skip1 = false;
						skip2 = true;
					}
				} else if (section2 != null && fromTwo) {
					dataType = dataType2;
					addObjectsList.add(new CatWrapper(section2.getSortedEntries(), prefix2.copyAppend("2")));
					countObjectsList.add(new CatWrapper(section2.getSortedEntries(), prefix2.copyAppend("2")));
					if (!hmIterator2.hasNext()) {
						section2 = null;
						dataType2 = ByteString.empty();
						skip1 = false;
					} else {
						skip1 = true;
						skip2 = false;
					}
				}
			}
			long numberElts = 0;
			CatUnion itCountObjects = new CatUnion(countObjectsList);
			while (itCountObjects.hasNext()) {
				numberElts++;
				itCountObjects.next();
			}

			CatUnion itAddObjects = new CatUnion(addObjectsList);
			// subtract the number of objects to be skipped - if creating do
			// data type section
			if (dataType.equals(NO_DT_OBJECTS)) {
				numberElts -= numSkipObjects;
				catSection(numberElts, type, itAddObjects, skipObject, allMappings, iListener);
			} else // if catting literals sections .. nothing will move (nothing
					// to be skipped)
				catSection(numberElts, type, itAddObjects, new CatUnion(new ArrayList<>()), allMappings, iListener);
			if (numberElts > 0) {
				dataTypes.add(dataType);
				offsets.put(dataType, total);
			}
			total += numberElts;
			type++;
		}
//        System.out.println("SHARED-------------------");
		iListener = PrefixListener.of("Generate shared: ", listener);
		if (iListener != null) {
			iListener.notifyProgress(0, "start");
		}

		int numCommonS1O2 = 0;
		if (allObjects2.containsKey(NO_DT_OBJECTS)) {
			CatIntersection i2 = new CatIntersection(
					new CatWrapper(dictionary1.getSubjects().getSortedEntries(), SectionUtil.S1),
					new CatWrapper(allObjects2.get(NO_DT_OBJECTS).getSortedEntries(), NO_DT_OBJECTS_2));
			while (i2.hasNext()) {
				i2.next();
				numCommonS1O2++;
			}
		}
		int numCommonO1S2 = 0;
		if (allObjects1.containsKey(NO_DT_OBJECTS)) {
			CatIntersection i2 = new CatIntersection(
					new CatWrapper(allObjects1.get(NO_DT_OBJECTS).getSortedEntries(), NO_DT_OBJECTS_1),
					new CatWrapper(dictionary2.getSubjects().getSortedEntries(), SectionUtil.S2));
			while (i2.hasNext()) {
				i2.next();
				numCommonO1S2++;
			}
		}

		CatIntersection i2 = new CatIntersection(
				new CatWrapper(dictionary1.getShared().getSortedEntries(), SectionUtil.SH1),
				new CatWrapper(dictionary2.getShared().getSortedEntries(), SectionUtil.SH2));
		int numCommonSh1Sh2 = 0;
		while (i2.hasNext()) {
			i2.next();
			numCommonSh1Sh2++;
		}
		numShared = dictionary1.getShared().getNumberOfElements() + dictionary2.getShared().getNumberOfElements()
				- numCommonSh1Sh2 + numCommonS1O2 + numCommonO1S2;

		ArrayList<Iterator<CatElement>> addSharedList = new ArrayList<>();
		addSharedList.add(new CatWrapper(dictionary1.getShared().getSortedEntries(), SectionUtil.SH1));
		addSharedList.add(new CatWrapper(dictionary2.getShared().getSortedEntries(), SectionUtil.SH2));

		if (allObjects1.containsKey(NO_DT_OBJECTS)) {
			addSharedList.add(new CatIntersection(
					new CatWrapper(allObjects1.get(NO_DT_OBJECTS).getSortedEntries(), NO_DT_OBJECTS_1),
					new CatWrapper(dictionary2.getShared().getSortedEntries(), SectionUtil.SH2)));
			addSharedList.add(
					new CatIntersection(new CatWrapper(dictionary2.getSubjects().getSortedEntries(), SectionUtil.S2),
							new CatWrapper(allObjects1.get(NO_DT_OBJECTS).getSortedEntries(), NO_DT_OBJECTS_1)));
		}
		if (allObjects2.containsKey(NO_DT_OBJECTS)) {
			addSharedList.add(
					new CatIntersection(new CatWrapper(dictionary1.getSubjects().getSortedEntries(), SectionUtil.S1),
							new CatWrapper(allObjects2.get(NO_DT_OBJECTS).getSortedEntries(), NO_DT_OBJECTS_2)));
			addSharedList.add(new CatIntersection(
					new CatWrapper(allObjects2.get(NO_DT_OBJECTS).getSortedEntries(), NO_DT_OBJECTS_2),
					new CatWrapper(dictionary1.getShared().getSortedEntries(), SectionUtil.SH1)));
		}
		addSharedList
				.add(new CatIntersection(new CatWrapper(dictionary1.getSubjects().getSortedEntries(), SectionUtil.S1),
						new CatWrapper(dictionary2.getShared().getSortedEntries(), SectionUtil.SH2)));
		addSharedList
				.add(new CatIntersection(new CatWrapper(dictionary2.getSubjects().getSortedEntries(), SectionUtil.S2),
						new CatWrapper(dictionary1.getShared().getSortedEntries(), SectionUtil.SH1)));

		CatUnion itAddShared = new CatUnion(addSharedList);
		catSection(numShared, 1, itAddShared, new CatUnion(new ArrayList<>()), allMappings, iListener);

		// Putting the sections together
		ControlInfo ci = new ControlInformation();
		ci.setType(ControlInfo.Type.DICTIONARY);
		ci.setFormat(HDTVocabulary.DICTIONARY_TYPE_FOUR_SECTION);
		ci.setInt("elements", numSubjects + numPredicates + numObject + numShared);

		try (FileOutputStream outFinal = new FileOutputStream(location + "dictionary")) {
			ci.save(outFinal);
			for (int i = 1; i <= 3; i++) {
				Files.copy(Path.of(location + "section" + i), outFinal);
				Files.delete(Path.of(location + "section" + i));
			}
			VByte.encode(outFinal, dataTypes.size());
			for (ByteString datatype : dataTypes) {
				String datatypeStr = datatype.toString();
				byte[] bytes = datatypeStr.getBytes();
				IOUtil.writeSizedBuffer(outFinal, bytes, 0, bytes.length, iListener);
			}
			for (int i = 0; i < dataTypes.size(); i++) {
				Files.copy(Path.of(location + "section" + (4 + i)), outFinal);
				Files.delete(Path.of(location + "section" + (4 + i)));
			}
		}
		// create the objects mappings
		long oldId = 0;
		hmIterator1 = allObjects1.entrySet().iterator();
		countSubSections1 = 0;
		countSubSections2 = 0;
		while (hmIterator1.hasNext()) {
			Map.Entry<? extends CharSequence, DictionarySection> entry = hmIterator1.next();
			ByteString dataType = ByteString.of(entry.getKey());
			ByteString prefix;
			if (dataType.equals(NO_DT_OBJECTS)) {
				prefix = NO_DT_OBJECTS;
			} else {
				prefix = SectionUtil.createSub(countSubSections1);
			}
			prefix = prefix.copyAppend("1");
			if (allMappings.containsKey(prefix)) {
				CatMapping mapping = allMappings.get(prefix);
				for (int i = 0; i < mapping.getSize(); i++) {
					long newId = mapping.getMapping(i);
					if (mapping.getType(i) != 1 && offsets.containsKey(dataType)) {
						newId = newId + offsets.get(dataType);
					}
					allMappings.get(SectionUtil.O1).set(oldId, newId, (int) mapping.getType(i));
					oldId++;
				}
			}
			countSubSections1++;
		}

		oldId = 0;
		hmIterator2 = allObjects2.entrySet().iterator();
		while (hmIterator2.hasNext()) {
			Map.Entry<? extends CharSequence, DictionarySection> entry = hmIterator2.next();
			ByteString dataType = ByteString.of(entry.getKey());
			ByteString prefix;
			if (dataType.equals(NO_DT_OBJECTS)) {
				prefix = NO_DT_OBJECTS;
			} else {
				prefix = SectionUtil.createSub(countSubSections2);
			}
			prefix = prefix.copyAppend("2");
			CatMapping mapping = allMappings.get(prefix);
			if (mapping != null) {
				countSubSections2++;
				for (int i = 0; i < mapping.getSize(); i++) {
					long newId = mapping.getMapping(i);
					long mappingType = mapping.getType(i);
					Long offset = offsets.get(dataType);
					if (mappingType != 1 && offset != null) {
						newId = newId + offset;
					}
					allMappings.get(SectionUtil.O2).set(oldId, newId, (int) mappingType);
					oldId++;
				}
			}
		}
		// calculate the inverse mapping for the subjects, i.e. from the new
		// dictionary subject section to the old ones
		mappingS = new CatMappingBack(location, numSubjects + numShared);

		for (int i = 0; i < allMappings.get(SectionUtil.SH1).getSize(); i++) {
			mappingS.set(allMappings.get(SectionUtil.SH1).getMapping(i), i + 1, 1);
		}

		for (int i = 0; i < allMappings.get(SectionUtil.SH2).getSize(); i++) {
			mappingS.set(allMappings.get(SectionUtil.SH2).getMapping(i), i + 1, 2);
		}

		for (int i = 0; i < allMappings.get(SectionUtil.S1).getSize(); i++) {
			if (allMappings.get(SectionUtil.S1).getType(i) == 1) {
				mappingS.set(allMappings.get(SectionUtil.S1).getMapping(i), (i + 1 + (int) dictionary1.getNshared()),
						1);
			} else {
				mappingS.set(allMappings.get(SectionUtil.S1).getMapping(i) + (int) numShared,
						(i + 1 + (int) dictionary1.getNshared()), 1);
			}
		}

		for (int i = 0; i < allMappings.get(SectionUtil.S2).getSize(); i++) {
			if (allMappings.get(SectionUtil.S2).getType(i) == 1) {
				mappingS.set(allMappings.get(SectionUtil.S2).getMapping(i), (i + 1 + (int) dictionary2.getNshared()),
						2);
			} else {
				mappingS.set(allMappings.get(SectionUtil.S2).getMapping(i) + (int) numShared,
						(i + 1 + (int) dictionary2.getNshared()), 2);
			}
		}
	}

	@Override
	public void close() throws IOException {
		// iterate over all mappings and close them
		try {
			IOUtil.closeAll(allMappings.values());
		} finally {
			IOUtil.closeAll(mappingS);
		}
	}

	private void catSection(long numEntries, int type, CatUnion itAdd, CatUnion itSkip,
			Map<ByteString, CatMapping> mappings, ProgressListener listener) throws IOException {
		long numberElements = 0;
		ByteString name;
		switch (type) {
		case 2:
			name = SectionUtil.SECTION_SUBJECT;
			break;
		case 3:
			name = SectionUtil.SECTION_OBJECT;
			break;
		case 4:
			name = SectionUtil.SECTION_PREDICATE;
			break;
		default:
			name = CompactString.EMPTY;
			break;
		}
		long storedBuffersSize = 0;
		long numBlocks = 0;
		SequenceLog64BigDisk blocks;
		ByteArrayOutputStream byteOut;
		try (CRCOutputStream outBuffer = new CRCOutputStream(new FileOutputStream(location + "section_buffer_" + type),
				new CRC32())) {
			blocks = new SequenceLog64BigDisk(location + "SequenceLog64BigDisk" + type, 64, numEntries / 16);
			byteOut = new ByteArrayOutputStream(16 * 1024);
			if (numEntries > 0) {
				ByteString previousStr = null;

				CatElement skipElement = null;
				if (itSkip.hasNext()) {
					skipElement = itSkip.next();
				}
				while (itAdd.hasNext()) {
					ListenerUtil.notifyCond(listener, "Analyze section " + name + " ", numberElements, numberElements,
							numEntries);
					CatElement nextElement = itAdd.next();
					if (skipElement != null && nextElement.entity.equals(skipElement.entity)) {
						if (itSkip.hasNext())
							skipElement = itSkip.next();
						else
							skipElement = null;
					} else {
						for (int i = 0; i < nextElement.IDs.size(); i++) {
							long id = nextElement.IDs.get(i).pos;
							ByteString iter = nextElement.IDs.get(i).iter;
							mappings.get(iter).set(id - 1, numberElements + 1, type);
						}

						ByteString str = nextElement.entity;
						if (numberElements % DEFAULT_BLOCK_SIZE == 0) {
							blocks.append(storedBuffersSize + byteOut.size());
							numBlocks++;

							// if a buffer is filled, flush the byteOut and
							// store it
							if (((numBlocks - 1) % BLOCK_PER_BUFFER == 0) && ((numBlocks - 1) / BLOCK_PER_BUFFER != 0)
									|| byteOut.size() > 200000) {
								storedBuffersSize += byteOut.size();
								byteOut.flush();
								byte[] array = byteOut.toByteArray();
								IOUtil.writeBuffer(outBuffer, array, 0, array.length, null);
								byteOut.close();
								byteOut = new ByteArrayOutputStream(16 * 1024);
							}

							// Copy full string
							ByteStringUtil.append(byteOut, str, 0);
						} else {
							// Find common part.
							int delta = ByteStringUtil.longestCommonPrefix(previousStr, str);
							// Write Delta in VByte
							VByte.encode(byteOut, delta);
							// Write remaining
							ByteStringUtil.append(byteOut, str, delta);
						}
						byteOut.write(0); // End of string
						previousStr = str;
						numberElements += 1;
					}
				}
			}
			// Ending block pointer.
			blocks.append(storedBuffersSize + byteOut.size());
			// Trim text/blocks
			blocks.aggressiveTrimToSize();
			byteOut.flush();
			// section.addBuffer(buffer, byteOut.toByteArray());
			byte[] bytes = byteOut.toByteArray();
			IOUtil.writeBuffer(outBuffer, bytes, 0, bytes.length, null);
			outBuffer.writeCRC();
		}
		// Save the section conforming to the HDT format
		try (CRCOutputStream out = new CRCOutputStream(new FileOutputStream(location + "section" + type), new CRC8())) {
			// write the index type
			out.write(2);
			// write the number of strings
			VByte.encode(out, numberElements);
			// write the datasize
			VByte.encode(out, storedBuffersSize + byteOut.size());
			// wirte the blocksize
			VByte.encode(out, DEFAULT_BLOCK_SIZE);
			// write CRC
			out.writeCRC();
			// write the blocks
			blocks.save(out, null); // Write blocks directly to output, they
									// have their own CRC check.
			blocks.close();
			// write out_buffer
			Files.copy(Path.of(location + "section_buffer_" + type), out);
		}
		Files.deleteIfExists(Paths.get(location + "section_buffer_" + type));
		Files.deleteIfExists(Paths.get(location + "SequenceLog64BigDisk" + type));
	}

	public CatMappingBack getMappingS() {
		return mappingS;
	}

	@Override
	public Map<ByteString, CatMapping> getAllMappings() {
		return allMappings;
	}

	public long getNumShared() {
		return numShared;
	}
}
