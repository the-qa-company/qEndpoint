package com.the_qa_company.qendpoint.core.dictionary.impl;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.compact.bitmap.ModifiableBitmap;
import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.dictionary.DictionaryDiff;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilCat.CatElement;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilCat.CatIntersection;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilCat.CatMapping;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilCat.CatUnion;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilCat.SectionUtil;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilDiff.DiffWrapper;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.ControlInfo;
import com.the_qa_company.qendpoint.core.options.ControlInformation;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MultipleSectionDictionaryDiff implements DictionaryDiff {

	private final String location;
	private final Map<ByteString, CatMapping> allMappings = new HashMap<>();
	private CatMapping mappingBack;
	public long numShared;

	public MultipleSectionDictionaryDiff(String location) {
		this.location = location;
	}

	@Override
	public void close() throws IOException {
		// iterate over all mappings and close them
		try {
			IOUtil.closeAll(allMappings.values());
		} finally {
			IOUtil.closeAll(mappingBack);
		}
	}

	@Override
	public void diff(Dictionary dictionary, Map<CharSequence, ModifiableBitmap> bitmaps, ProgressListener listener)
			throws IOException {
		allMappings.put(SectionUtil.SECTION_PREDICATE, new CatMapping(location, SectionUtil.SECTION_PREDICATE,
				dictionary.getPredicates().getNumberOfElements()));
		allMappings.put(SectionUtil.SECTION_SUBJECT,
				new CatMapping(location, SectionUtil.SECTION_SUBJECT, dictionary.getSubjects().getNumberOfElements()));
		int countSubSection = 0;

		for (Map.Entry<? extends CharSequence, DictionarySection> next : dictionary.getAllObjects().entrySet()) {
			ByteString subPrefix;
			if (next.getKey().equals(LiteralsUtils.NO_DATATYPE)) {
				subPrefix = LiteralsUtils.NO_DATATYPE;
			} else {
				subPrefix = SectionUtil.createSub(countSubSection);
			}
			allMappings.put(subPrefix, new CatMapping(location, subPrefix, next.getValue().getNumberOfElements()));
			countSubSection++;
		}
		allMappings.put(SectionUtil.SECTION_OBJECT,
				new CatMapping(location, SectionUtil.SECTION_OBJECT, dictionary.getNAllObjects()));
		allMappings.put(SectionUtil.SECTION_SHARED,
				new CatMapping(location, SectionUtil.SECTION_SHARED, dictionary.getShared().getNumberOfElements()));
//        allMappings.put("shared_o",new CatMapping(location,"shared_o",dictionary.getShared().getNumberOfElements()));

		// Predicates
		Bitmap predicatesBitMap = bitmaps.get("P");

		Iterator<? extends CharSequence> predicates = dictionary.getPredicates().getSortedEntries();
//        CatWrapper itAddPreds = new CatWrapper(predicates,"predicate");
		DiffWrapper itSkipPreds = new DiffWrapper(predicates, predicatesBitMap, SectionUtil.SECTION_PREDICATE);

//        ArrayList<Iterator<CatElement>> listAddPred = new ArrayList<>();
//        listAddPred.add(itAddPreds);

		ArrayList<Iterator<CatElement>> listSkipPred = new ArrayList<>();
		listSkipPred.add(itSkipPreds);

		long numPreds = predicatesBitMap.countOnes();

		SectionUtil.createSection(location, numPreds, 3, new CatUnion(listSkipPred), new CatUnion(new ArrayList<>()),
				allMappings, 0, listener);
		// diffSection(numPreds,4,predicates,predicatesBitMap,allMappings,listener);

		// Subjects
		Bitmap subjectsBitMap = bitmaps.get("S");
		Iterator<? extends CharSequence> subjects = dictionary.getSubjects().getSortedEntries();

//        CatWrapper itAddSubs = new CatWrapper(subjects,"subject");
		DiffWrapper itSkipSubs = new DiffWrapper(subjects, subjectsBitMap, SectionUtil.SECTION_SUBJECT);

//        ArrayList<Iterator<CatElement>> listAddSubj = new ArrayList<>();
//        listAddSubj.add(itAddSubs);

		ArrayList<Iterator<CatElement>> listSkipSubj = new ArrayList<>();
		listSkipSubj.add(itSkipSubs);

		SharedWrapper sharedWrapper = new SharedWrapper(0, bitmaps.get("SH_S"), bitmaps.get("SH_O"),
				dictionary.getShared().getSortedEntries());
		long numNewSubj = sharedWrapper.count();
		sharedWrapper = new SharedWrapper(0, bitmaps.get("SH_S"), bitmaps.get("SH_O"),
				dictionary.getShared().getSortedEntries());
		listSkipSubj.add(sharedWrapper);

		long numSubj = subjectsBitMap.countOnes() + numNewSubj;
		SectionUtil.createSection(location, numSubj, 2, new CatUnion(listSkipSubj), new CatUnion(new ArrayList<>()),
				allMappings, 0, listener);
		// diffSection(numSubj,2,subjects,subjectsBitMap,allMappings,listener);

		// Objects
		// ----------------------------+++++++++++++++++++++++++++++++++----------------------------------------
		List<ByteString> dataTypes = new ArrayList<>();
		Map<ByteString, Long> offsets = new HashMap<>();

		int countSection = 0;
		long totalObjects = 0;
		for (Map.Entry<? extends CharSequence, DictionarySection> next : dictionary.getAllObjects().entrySet()) {
			int type = 4 + dataTypes.size();
			ByteString key = ByteString.of(next.getKey());
			if (key.equals(LiteralsUtils.NO_DATATYPE)) {
				long numNoDataType = createNoDataTypeSection(bitmaps, dictionary, totalObjects, type);
				if (numNoDataType > 0) {
					dataTypes.add(LiteralsUtils.NO_DATATYPE);
					offsets.put(LiteralsUtils.NO_DATATYPE, totalObjects);
					totalObjects += numNoDataType;
				}
			} else {
				Bitmap objectsBitMap = bitmaps.get(key);
				Iterator<? extends CharSequence> objects = dictionary.getAllObjects().get(key).getSortedEntries();

				ByteString subPrefix = SectionUtil.createSub(countSection);
				DiffWrapper itSkipObjs = new DiffWrapper(objects, objectsBitMap, subPrefix);

				ArrayList<Iterator<CatElement>> listSkipObjs = new ArrayList<>();
				listSkipObjs.add(itSkipObjs);

				long numObject = objectsBitMap.countOnes();
				// append the data types of the new dictionary if the section
				// still exists ( number of elts > 0 )
				if (numObject > 0) {
					dataTypes.add(key);
					offsets.put(key, totalObjects);
				}
				totalObjects += numObject;
				SectionUtil.createSection(location, numObject, type, new CatUnion(listSkipObjs),
						new CatUnion(new ArrayList<>()), allMappings, 0, null);
			}
			countSection++;
		}

		// Shared
		Bitmap sharedSubjBitMap = bitmaps.get("SH_S");
		Bitmap sharedObjBitMap = bitmaps.get("SH_O");

		Iterator<? extends CharSequence> shared = dictionary.getShared().getSortedEntries();

		DiffWrapper sharedSubj = new DiffWrapper(shared, sharedSubjBitMap, SectionUtil.SECTION_SHARED);

		shared = dictionary.getShared().getSortedEntries();

		DiffWrapper sharedObj = new DiffWrapper(shared, sharedObjBitMap, SectionUtil.SECTION_SHARED);

		ArrayList<Iterator<CatElement>> listShared = new ArrayList<>();
		listShared.add(new CatIntersection(sharedSubj, sharedObj));

		CatUnion union = new CatUnion(listShared);
		while (union.hasNext()) {
			union.next();
			numShared++;
		}

		listShared = new ArrayList<>();
		sharedSubj = new DiffWrapper(dictionary.getShared().getSortedEntries(), sharedSubjBitMap,
				SectionUtil.SECTION_SHARED);

		sharedObj = new DiffWrapper(dictionary.getShared().getSortedEntries(), sharedObjBitMap,
				SectionUtil.SECTION_SHARED);
		listShared.add(new CatIntersection(sharedSubj, sharedObj));

		SectionUtil.createSection(location, numShared, 1, new CatUnion(listShared), new CatUnion(new ArrayList<>()),
				allMappings, 0, listener);
		// diffSection(numShared,1,shared,sharedBitMap,allMappings,listener);

		// Putting the sections together
		ControlInfo ci = new ControlInformation();
		ci.setType(ControlInfo.Type.DICTIONARY);
		ci.setFormat(dictionary.getType());

		ci.setInt("elements", numSubj + numPreds + totalObjects + numShared);

		try (OutputStream out = new FileOutputStream(location + "dictionary")) {
			ci.save(out);
			for (int i = 1; i <= 3; i++) {
				Files.copy(Path.of(location + "section" + i), out);
				Files.delete(Path.of(location + "section" + i));
			}
			VByte.encode(out, dataTypes.size());
			for (ByteString datatype : dataTypes) {
				IOUtil.writeSizedBuffer(out, datatype.getBuffer(), listener);
			}
			for (int i = 0; i < dataTypes.size(); i++) {
				Files.copy(Path.of(location + "section" + (4 + i)), out);
				Files.delete(Path.of(location + "section" + (4 + i)));
			}
		}
		// create global objects mapping from section by section mappings
		long oldId = 0;
		countSection = 0;
		for (CharSequence dataType : dictionary.getAllObjects().keySet()) {
			ByteString subPrefix;
			ByteString dataTypeB = ByteString.of(dataType);
			if (dataTypeB.equals(LiteralsUtils.NO_DATATYPE)) {
				subPrefix = LiteralsUtils.NO_DATATYPE;
			} else {
				subPrefix = SectionUtil.createSub(countSection);
			}

			if (allMappings.containsKey(subPrefix)) {
				CatMapping mapping = allMappings.get(subPrefix);
				for (int i = 0; i < mapping.getSize(); i++) {
					long newId = mapping.getMapping(i);
					Long offset;
					if (mapping.getType(i) != 1 && (offset = offsets.get(dataTypeB)) != null) {
						newId = newId + offset;
					}
					allMappings.get(SectionUtil.SECTION_OBJECT).set(oldId, newId, (int) mapping.getType(i));
					oldId++;
				}
			}
			countSection++;
		}
		mappingBack = new CatMapping(location, SectionUtil.BACK, numSubj + numShared);

		if (mappingBack.getSize() > 0) {
			for (int i = 0; i < allMappings.get(SectionUtil.SECTION_SHARED).getSize(); i++) {
				long type = allMappings.get(SectionUtil.SECTION_SHARED).getType(i);
				if (type == 1) {
					mappingBack.set(allMappings.get(SectionUtil.SECTION_SHARED).getMapping(i) - 1, i + 1, 1);
				} else if (type == 2) {
					mappingBack.set(allMappings.get(SectionUtil.SECTION_SHARED).getMapping(i) + numShared - 1, i + 1,
							2);
				}
			}

			for (int i = 0; i < allMappings.get(SectionUtil.SECTION_SUBJECT).getSize(); i++) {
				long type = allMappings.get(SectionUtil.SECTION_SUBJECT).getType(i);
				if (type == 1) {
					mappingBack.set(allMappings.get(SectionUtil.SECTION_SUBJECT).getMapping(i) - 1,
							(i + 1 + (int) dictionary.getNshared()), 1);
				} else if (type == 2) {
					mappingBack.set(allMappings.get(SectionUtil.SECTION_SUBJECT).getMapping(i) + numShared - 1,
							(i + 1 + (int) dictionary.getNshared()), 2);
				}
			}
		}

	}

	private long createNoDataTypeSection(Map<CharSequence, ModifiableBitmap> bitmaps, Dictionary dictionary,
			long numObjectsAlreadyAdded, int type) throws IOException {
		Bitmap objectsBitMap = bitmaps.get(LiteralsUtils.NO_DATATYPE);
		Iterator<? extends CharSequence> objects = dictionary.getAllObjects().get(LiteralsUtils.NO_DATATYPE)
				.getSortedEntries();

		DiffWrapper itSkipObjs = new DiffWrapper(objects, objectsBitMap, LiteralsUtils.NO_DATATYPE);

		ArrayList<Iterator<CatElement>> listSkipObjs = new ArrayList<>();
		listSkipObjs.add(itSkipObjs);

		// flag = 1 for objects
		SharedWrapper sharedWrapper = new SharedWrapper(1, bitmaps.get("SH_S"), bitmaps.get("SH_O"),
				dictionary.getShared().getSortedEntries());
		long numNewObj = sharedWrapper.count();
		sharedWrapper = new SharedWrapper(1, bitmaps.get("SH_S"), bitmaps.get("SH_O"),
				dictionary.getShared().getSortedEntries());
		listSkipObjs.add(sharedWrapper);

		long numObject = objectsBitMap.countOnes() + numNewObj;

		SectionUtil.createSection(location, numObject, type, new CatUnion(listSkipObjs),
				new CatUnion(new ArrayList<>()), allMappings, numObjectsAlreadyAdded, null);
		return numObject;
	}

	private static class SharedWrapper implements Iterator<CatElement> {
		private final Bitmap bitmapSub;
		private final Bitmap bitmapObj;
		private final Iterator<? extends CharSequence> sectionIter;
		private final int flag; // 0 = subject ; 1 = object

		public SharedWrapper(int flag, Bitmap bitmapSub, Bitmap bitmapObj,
				Iterator<? extends CharSequence> sectionIter) {
			this.bitmapSub = bitmapSub;
			this.bitmapObj = bitmapObj;
			this.sectionIter = sectionIter;
			this.flag = flag;
		}

		CatElement next;
		long count = 0;

		@Override
		public boolean hasNext() {

			while (sectionIter.hasNext()) {
				ByteString element = ByteString.of(sectionIter.next());

				if ((flag == 0 && bitmapSub.access(count) && !bitmapObj.access(count))
						|| (flag == 1 && bitmapObj.access(count) && !bitmapSub.access(count))) {
					ArrayList<CatElement.IteratorPlusPosition> IDs = new ArrayList<>();
					IDs.add(new CatElement.IteratorPlusPosition(SectionUtil.SECTION_SHARED, count + 1));
					next = new CatElement(element, IDs);
					count++;
					return true;
				}

				count++;
			}
			return false;
		}

		@Override
		public CatElement next() {
			return next;
		}

		public int count() {
			int i = 0;
			while (hasNext()) {
				// next();
				i++;
			}
			return i;
		}
	}

	@Override
	public Map<ByteString, CatMapping> getAllMappings() {
		return allMappings;
	}

	public CatMapping getMappingBack() {
		return mappingBack;
	}

	@Override
	public long getNumShared() {
		return numShared;
	}
}
