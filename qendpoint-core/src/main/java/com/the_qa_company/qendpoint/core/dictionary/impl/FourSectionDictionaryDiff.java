package com.the_qa_company.qendpoint.core.dictionary.impl;

import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.dictionary.DictionaryDiff;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilCat.CatElement;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilCat.CatIntersection;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilCat.CatMapping;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilCat.CatUnion;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilCat.SectionUtil;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilDiff.DiffWrapper;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.ControlInfo;
import com.the_qa_company.qendpoint.core.options.ControlInformation;
import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.compact.bitmap.ModifiableBitmap;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class FourSectionDictionaryDiff implements DictionaryDiff {

	private final String location;
	private final Map<ByteString, CatMapping> allMappings = new HashMap<>();
	private CatMapping mappingBack;
	public long numShared;

	public FourSectionDictionaryDiff(String location) {
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
		allMappings.put(SectionUtil.SECTION_OBJECT,
				new CatMapping(location, SectionUtil.SECTION_OBJECT, dictionary.getObjects().getNumberOfElements()));
		allMappings.put(SectionUtil.SECTION_SHARED,
				new CatMapping(location, SectionUtil.SECTION_SHARED, dictionary.getShared().getNumberOfElements()));
//        allMappings.put("shared_o",new CatMapping(location,"shared_o",dictionary.getShared().getNumberOfElements()));

		// Predicates
		Bitmap predicatesBitMap = bitmaps.get("P");

		Iterator<? extends CharSequence> predicates = dictionary.getPredicates().getSortedEntries();
		DiffWrapper itSkipPreds = new DiffWrapper(predicates, predicatesBitMap, SectionUtil.SECTION_PREDICATE);

		ArrayList<Iterator<CatElement>> listSkipPred = new ArrayList<>();
		listSkipPred.add(itSkipPreds);

		long numPreds = predicatesBitMap.countOnes();

		SectionUtil.createSection(location, numPreds, 4, new CatUnion(listSkipPred), new CatUnion(new ArrayList<>()),
				allMappings, 0, listener);
		// diffSection(numPreds,4,predicates,predicatesBitMap,allMappings,listener);

		// Subjects
		Bitmap subjectsBitMap = bitmaps.get("S");
		Iterator<? extends CharSequence> subjects = dictionary.getSubjects().getSortedEntries();

		DiffWrapper itSkipSubs = new DiffWrapper(subjects, subjectsBitMap, SectionUtil.SECTION_SUBJECT);

		List<Iterator<CatElement>> listSkipSubj = new ArrayList<>();
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

		// Objects
		// ----------------------------+++++++++++++++++++++++++++++++++----------------------------------------

		Bitmap objectsBitMap = bitmaps.get("O");
		Iterator<? extends CharSequence> objects = dictionary.getObjects().getSortedEntries();

		DiffWrapper itSkipObjs = new DiffWrapper(objects, objectsBitMap, SectionUtil.SECTION_OBJECT);

		ArrayList<Iterator<CatElement>> listSkipObjs = new ArrayList<>();
		listSkipObjs.add(itSkipObjs);

		// flag = 1 for objects
		sharedWrapper = new SharedWrapper(1, bitmaps.get("SH_S"), bitmaps.get("SH_O"),
				dictionary.getShared().getSortedEntries());
		long numNewObj = sharedWrapper.count();
		sharedWrapper = new SharedWrapper(1, bitmaps.get("SH_S"), bitmaps.get("SH_O"),
				dictionary.getShared().getSortedEntries());
		listSkipObjs.add(sharedWrapper);

		long numObject = objectsBitMap.countOnes() + numNewObj;

		SectionUtil.createSection(location, numObject, 3, new CatUnion(listSkipObjs), new CatUnion(new ArrayList<>()),
				allMappings, 0, listener);
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

		ci.setInt("elements", numSubj + numPreds + numObject + numShared);

		try (OutputStream outFinal = new FileOutputStream(location + "dictionary")) {
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
		mappingBack = new CatMapping(location, SectionUtil.BACK, numSubj + numShared);

		if (mappingBack.getSize() > 0) {
			CatMapping sharedMapping = allMappings.get(SectionUtil.SECTION_SHARED);
			for (int i = 0; i < sharedMapping.getSize(); i++) {
				long type = sharedMapping.getType(i);
				if (type == 1) {
					mappingBack.set(sharedMapping.getMapping(i) - 1, i + 1, 1);
				} else if (type == 2) {
					mappingBack.set(sharedMapping.getMapping(i) + numShared - 1, i + 1, 2);
				}
			}

			CatMapping subjectMapping = allMappings.get(SectionUtil.SECTION_SUBJECT);
			for (int i = 0; i < subjectMapping.getSize(); i++) {
				long type = subjectMapping.getType(i);
				if (type == 1) {
					mappingBack.set(subjectMapping.getMapping(i) - 1, (i + 1 + (int) dictionary.getNshared()), 1);
				} else if (type == 2) {
					mappingBack.set(subjectMapping.getMapping(i) + numShared - 1,
							(i + 1 + (int) dictionary.getNshared()), 2);
				}
			}
		}
	}

	private static class SharedWrapper implements Iterator<CatElement> {
		private final Bitmap bitmapSub;
		private final Bitmap bitmapObj;
		private final Iterator<? extends CharSequence> sectionIter;
		private final int flag; // 0 = subject ; 1 = object
		private CatElement next;
		private long count = 0;

		public SharedWrapper(int flag, Bitmap bitmapSub, Bitmap bitmapObj,
				Iterator<? extends CharSequence> sectionIter) {
			this.bitmapSub = bitmapSub;
			this.bitmapObj = bitmapObj;
			this.sectionIter = sectionIter;
			this.flag = flag;
		}

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

	@Override
	public CatMapping getMappingBack() {
		return mappingBack;
	}

	@Override
	public long getNumShared() {
		return numShared;
	}
}
