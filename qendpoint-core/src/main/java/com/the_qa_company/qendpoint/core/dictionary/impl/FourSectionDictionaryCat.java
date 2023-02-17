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
 * jose.gimenez.garcia@univ-st-etienne.fr Ali Haidar: ali.haidar@qanswer.eu
 */

package com.the_qa_company.qendpoint.core.dictionary.impl;

import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.dictionary.DictionaryCat;
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
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.listener.PrefixListener;
import com.the_qa_company.qendpoint.core.util.string.ByteString;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class FourSectionDictionaryCat implements DictionaryCat {

	private final Map<ByteString, CatMapping> allMappings = new HashMap<>();
	private final String location;
	private long numShared;

	private CatMappingBack mappingS;

	public FourSectionDictionaryCat(String location) {
		this.location = location;
	}

	public void cat(com.the_qa_company.qendpoint.core.dictionary.Dictionary dictionary1, Dictionary dictionary2,
			ProgressListener listener) throws IOException {
		allMappings.put(SectionUtil.P1,
				new CatMapping(location, SectionUtil.P1, dictionary1.getPredicates().getNumberOfElements()));
		allMappings.put(SectionUtil.P2,
				new CatMapping(location, SectionUtil.P2, dictionary2.getPredicates().getNumberOfElements()));
		allMappings.put(SectionUtil.S1,
				new CatMapping(location, SectionUtil.S1, dictionary1.getSubjects().getNumberOfElements()));
		allMappings.put(SectionUtil.S2,
				new CatMapping(location, SectionUtil.S2, dictionary2.getSubjects().getNumberOfElements()));
		allMappings.put(SectionUtil.O1,
				new CatMapping(location, SectionUtil.O1, dictionary1.getObjects().getNumberOfElements()));
		allMappings.put(SectionUtil.O2,
				new CatMapping(location, SectionUtil.O2, dictionary2.getObjects().getNumberOfElements()));
		allMappings.put(SectionUtil.SH1,
				new CatMapping(location, SectionUtil.SH1, dictionary1.getShared().getNumberOfElements()));
		allMappings.put(SectionUtil.SH2,
				new CatMapping(location, SectionUtil.SH2, dictionary2.getShared().getNumberOfElements()));

//        System.out.println("PREDICATES-------------------");
		ProgressListener iListener;

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
			// ListenerUtil.notifyCond(iListener, "Analyze common predicates",
			// numCommonPredicates, numCommonPredicates, maxPredicates);
		}
		long numPredicates = dictionary1.getPredicates().getNumberOfElements()
				+ dictionary2.getPredicates().getNumberOfElements() - numCommonPredicates;

		ArrayList<Iterator<CatElement>> addPredicatesList = new ArrayList<>();
		addPredicatesList.add(new CatWrapper(dictionary1.getPredicates().getSortedEntries(), SectionUtil.P1));
		addPredicatesList.add(new CatWrapper(dictionary2.getPredicates().getSortedEntries(), SectionUtil.P2));
		CatUnion itAddPredicates = new CatUnion(addPredicatesList);
		SectionUtil.createSection(location, numPredicates, 4, itAddPredicates, new CatUnion(new ArrayList<>()),
				allMappings, 0, iListener);
//        System.out.println("SUBJECTS-------------------");
		iListener = PrefixListener.of("Generate subjects: ", listener);
		if (iListener != null) {
			iListener.notifyProgress(0, "start");
		}
		ArrayList<Iterator<CatElement>> skipSubjectList = new ArrayList<>();

		skipSubjectList
				.add(new CatIntersection(new CatWrapper(dictionary1.getSubjects().getSortedEntries(), SectionUtil.S1),
						new CatWrapper(dictionary2.getShared().getSortedEntries(), SectionUtil.SH2)));
		skipSubjectList
				.add(new CatIntersection(new CatWrapper(dictionary1.getSubjects().getSortedEntries(), SectionUtil.S1),
						new CatWrapper(dictionary2.getObjects().getSortedEntries(), SectionUtil.O2)));
		skipSubjectList
				.add(new CatIntersection(new CatWrapper(dictionary2.getSubjects().getSortedEntries(), SectionUtil.S2),
						new CatWrapper(dictionary1.getShared().getSortedEntries(), SectionUtil.SH1)));
		skipSubjectList
				.add(new CatIntersection(new CatWrapper(dictionary2.getSubjects().getSortedEntries(), SectionUtil.S2),
						new CatWrapper(dictionary1.getObjects().getSortedEntries(), SectionUtil.O1)));
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
		skipSubjectList
				.add(new CatIntersection(new CatWrapper(dictionary1.getSubjects().getSortedEntries(), SectionUtil.S1),
						new CatWrapper(dictionary2.getObjects().getSortedEntries(), SectionUtil.O2)));
		skipSubjectList
				.add(new CatIntersection(new CatWrapper(dictionary2.getSubjects().getSortedEntries(), SectionUtil.S2),
						new CatWrapper(dictionary1.getShared().getSortedEntries(), SectionUtil.SH1)));
		skipSubjectList
				.add(new CatIntersection(new CatWrapper(dictionary2.getSubjects().getSortedEntries(), SectionUtil.S2),
						new CatWrapper(dictionary1.getObjects().getSortedEntries(), SectionUtil.O1)));
		skipSubject = new CatUnion(skipSubjectList);

		ArrayList<Iterator<CatElement>> addSubjectsList = new ArrayList<>();
		addSubjectsList.add(new CatWrapper(dictionary1.getSubjects().getSortedEntries(), SectionUtil.S1));
		addSubjectsList.add(new CatWrapper(dictionary2.getSubjects().getSortedEntries(), SectionUtil.S2));
		CatUnion itAddSubjects = new CatUnion(addSubjectsList);

		SectionUtil.createSection(location, numSubjects, 2, itAddSubjects, skipSubject, allMappings, 0, iListener);

//        System.out.println("OBJECTS-------------------");
		iListener = PrefixListener.of("Generate objects: ", listener);
		if (iListener != null) {
			iListener.notifyProgress(0, "start");
		}
		ArrayList<Iterator<CatElement>> skipObjectsList = new ArrayList<>();
		skipObjectsList
				.add(new CatIntersection(new CatWrapper(dictionary1.getObjects().getSortedEntries(), SectionUtil.O1),
						new CatWrapper(dictionary2.getShared().getSortedEntries(), SectionUtil.SH2)));
		skipObjectsList
				.add(new CatIntersection(new CatWrapper(dictionary1.getObjects().getSortedEntries(), SectionUtil.O1),
						new CatWrapper(dictionary2.getSubjects().getSortedEntries(), SectionUtil.S2)));
		skipObjectsList
				.add(new CatIntersection(new CatWrapper(dictionary2.getObjects().getSortedEntries(), SectionUtil.O2),
						new CatWrapper(dictionary1.getShared().getSortedEntries(), SectionUtil.SH1)));
		skipObjectsList
				.add(new CatIntersection(new CatWrapper(dictionary2.getObjects().getSortedEntries(), SectionUtil.O2),
						new CatWrapper(dictionary1.getSubjects().getSortedEntries(), SectionUtil.S1)));

		CatUnion skipObject = new CatUnion(skipObjectsList);
		int numSkipObjects = 0;
		while (skipObject.hasNext()) {
			skipObject.next();
			numSkipObjects++;
		}

		int numCommonObjects = 0;
		CatIntersection commonO1O2 = new CatIntersection(
				new CatWrapper(dictionary1.getObjects().getSortedEntries(), SectionUtil.O1),
				new CatWrapper(dictionary2.getObjects().getSortedEntries(), SectionUtil.O2));
		while (commonO1O2.hasNext()) {
			commonO1O2.next();
			numCommonObjects++;
		}

		skipObjectsList = new ArrayList<>();
		skipObjectsList
				.add(new CatIntersection(new CatWrapper(dictionary1.getObjects().getSortedEntries(), SectionUtil.O1),
						new CatWrapper(dictionary2.getShared().getSortedEntries(), SectionUtil.SH2)));
		skipObjectsList
				.add(new CatIntersection(new CatWrapper(dictionary1.getObjects().getSortedEntries(), SectionUtil.O1),
						new CatWrapper(dictionary2.getSubjects().getSortedEntries(), SectionUtil.S2)));
		skipObjectsList
				.add(new CatIntersection(new CatWrapper(dictionary2.getObjects().getSortedEntries(), SectionUtil.O2),
						new CatWrapper(dictionary1.getShared().getSortedEntries(), SectionUtil.SH1)));
		skipObjectsList
				.add(new CatIntersection(new CatWrapper(dictionary2.getObjects().getSortedEntries(), SectionUtil.O2),
						new CatWrapper(dictionary1.getSubjects().getSortedEntries(), SectionUtil.S1)));

		skipObject = new CatUnion(skipObjectsList);

		long numObject = dictionary1.getObjects().getNumberOfElements() + dictionary2.getObjects().getNumberOfElements()
				- numCommonObjects - numSkipObjects;
		ArrayList<Iterator<CatElement>> addObjectsList = new ArrayList<>();
		addObjectsList.add(new CatWrapper(dictionary1.getObjects().getSortedEntries(), SectionUtil.O1));
		addObjectsList.add(new CatWrapper(dictionary2.getObjects().getSortedEntries(), SectionUtil.O2));
		CatUnion itAddObjects = new CatUnion(addObjectsList);

		SectionUtil.createSection(location, numObject, 3, itAddObjects, skipObject, allMappings, 0, iListener);

//        System.out.println("SHARED-------------------");
		iListener = PrefixListener.of("Generate shared: ", listener);
		if (iListener != null) {
			iListener.notifyProgress(0, "start");
		}
		CatIntersection i2 = new CatIntersection(
				new CatWrapper(dictionary1.getSubjects().getSortedEntries(), SectionUtil.S1),
				new CatWrapper(dictionary2.getObjects().getSortedEntries(), SectionUtil.O2));
		int numCommonS1O2 = 0;
		while (i2.hasNext()) {
			i2.next();
			numCommonS1O2++;
		}
		i2 = new CatIntersection(new CatWrapper(dictionary1.getObjects().getSortedEntries(), SectionUtil.O1),
				new CatWrapper(dictionary2.getSubjects().getSortedEntries(), SectionUtil.S2));
		int numCommonO1S2 = 0;
		while (i2.hasNext()) {
			i2.next();
			numCommonO1S2++;
		}

		i2 = new CatIntersection(new CatWrapper(dictionary1.getShared().getSortedEntries(), SectionUtil.SH1),
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

		addSharedList
				.add(new CatIntersection(new CatWrapper(dictionary1.getSubjects().getSortedEntries(), SectionUtil.S1),
						new CatWrapper(dictionary2.getObjects().getSortedEntries(), SectionUtil.O2)));
		addSharedList
				.add(new CatIntersection(new CatWrapper(dictionary1.getSubjects().getSortedEntries(), SectionUtil.S1),
						new CatWrapper(dictionary2.getShared().getSortedEntries(), SectionUtil.SH2)));
		addSharedList
				.add(new CatIntersection(new CatWrapper(dictionary2.getSubjects().getSortedEntries(), SectionUtil.S2),
						new CatWrapper(dictionary1.getObjects().getSortedEntries(), SectionUtil.O1)));
		addSharedList
				.add(new CatIntersection(new CatWrapper(dictionary2.getSubjects().getSortedEntries(), SectionUtil.S2),
						new CatWrapper(dictionary1.getShared().getSortedEntries(), SectionUtil.SH1)));
		addSharedList
				.add(new CatIntersection(new CatWrapper(dictionary1.getObjects().getSortedEntries(), SectionUtil.O1),
						new CatWrapper(dictionary2.getShared().getSortedEntries(), SectionUtil.SH2)));
		addSharedList
				.add(new CatIntersection(new CatWrapper(dictionary2.getObjects().getSortedEntries(), SectionUtil.O2),
						new CatWrapper(dictionary1.getShared().getSortedEntries(), SectionUtil.SH1)));

		CatUnion itAddShared = new CatUnion(addSharedList);
		SectionUtil.createSection(location, numShared, 1, itAddShared, new CatUnion(new ArrayList<>()), allMappings, 0,
				iListener);

		// Putting the sections together
		ControlInfo ci = new ControlInformation();
		ci.setType(ControlInfo.Type.DICTIONARY);
		ci.setFormat(HDTVocabulary.DICTIONARY_TYPE_FOUR_SECTION);
		ci.setInt("elements", numSubjects + numPredicates + numObject + numShared);

		try (FileOutputStream outFinal = new FileOutputStream(location + "dictionary")) {
			ci.save(outFinal);
			for (int i = 1; i <= 4; i++) {
				int j = i;
				if (i == 4) {
					j = 3;
				} else if (j == 3) {
					j = 4;
				}
				Files.copy(Path.of(location + "section" + j), outFinal);
				Files.delete(Paths.get(location + "section" + j));
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
