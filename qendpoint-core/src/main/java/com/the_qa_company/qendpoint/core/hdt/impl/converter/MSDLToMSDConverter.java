package com.the_qa_company.qendpoint.core.hdt.impl.converter;

import com.the_qa_company.qendpoint.core.compact.sequence.DynamicSequence;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64BigDisk;
import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.dictionary.DictionaryPrivate;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.WriteDictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.impl.MultipleSectionDictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.UnmodifiableDictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.DictionarySectionFactory;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.Converter;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.hdt.impl.HDTBase;
import com.the_qa_company.qendpoint.core.hdt.impl.HDTImpl;
import com.the_qa_company.qendpoint.core.header.HeaderPrivate;
import com.the_qa_company.qendpoint.core.header.PlainHeader;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.MapIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.MergeExceptionIterator;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.IndexedNode;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.TriplesPrivate;
import com.the_qa_company.qendpoint.core.triples.impl.OneReadTempTriples;
import com.the_qa_company.qendpoint.core.triples.impl.WriteBitmapTriples;
import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.CharSequenceComparator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class MSDLToMSDConverter implements Converter {
	@Override
	public String getDestinationType() {
		return HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS;
	}

	@Override
	public void convertHDTFile(HDT origin, Path destination, ProgressListener listener, HDTOptions options)
			throws IOException {

		// remove null
		listener = ProgressListener.ofNullable(listener);
		options = HDTOptions.ofNullable(options);

		if (!(origin.getTriples() instanceof TriplesPrivate tp)) {
			throw new IllegalArgumentException("Can't convert triples not implementing the TriplesPrivate interface");
		}
		TripleComponentOrder order = tp.getOrder();

		if (order.getSubjectMapping() == null || order.getSubjectMapping() == TripleComponentRole.OBJECT) {
			throw new IllegalArgumentException("Can't convert triples with order setting the objects in the subjects");
		}

		int bufferSize = options.getInt32(HDTOptionsKeys.LOADER_DISK_BUFFER_SIZE_KEY, CloseSuppressPath.BUFFER_SIZE);

		if (!HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG.equals(origin.getDictionary().getType())) {
			throw new IllegalArgumentException("Bad origin type! " + origin.getDictionary().getType());
		}

		Path wipDir = destination.resolveSibling(destination.getFileName() + ".tmp");
		int i = 0;
		// find an available name
		while (Files.exists(wipDir)) {
			wipDir = destination.resolveSibling(destination.getFileName() + "." + ++i + ".tmp");
		}
		try (CloseSuppressPath dir = CloseSuppressPath.of(wipDir)) {
			dir.closeWithDeleteRecurse();
			dir.mkdirs();

			long nObjects = origin.getDictionary().getNAllObjects();
			long nShared = origin.getDictionary().getNshared();
			try (SequenceLog64BigDisk objectMap = new SequenceLog64BigDisk(dir.resolve("objectMap"),
					BitUtil.log2(nObjects), nObjects + 1)) {
				objectMap.clear();
				Map<? extends CharSequence, DictionarySection> objectsAll = origin.getDictionary().getAllObjects();

				Map<ByteString, SectionElement> langObjects = new TreeMap<>(CharSequenceComparator.getInstance());
				TreeMap<ByteString, DictionarySectionPrivate> futureAllObjects = new TreeMap<>(
						CharSequenceComparator.getInstance());

				long langSectionSize = 0;

				long start = 1;

				Map<ByteString, Long> starts = new HashMap<>();
				starts.put(LiteralsUtils.NO_DATATYPE, start);
				DictionarySection ndt = objectsAll.get(LiteralsUtils.NO_DATATYPE);
				futureAllObjects.put(LiteralsUtils.NO_DATATYPE, UnmodifiableDictionarySectionPrivate.of(ndt));
				if (ndt != null) {
					start += ndt.getNumberOfElements();
				}

				for (var e : objectsAll.entrySet()) {
					ByteString keyBS = ByteString.of(e.getKey());
					DictionarySection section = e.getValue();

					if (keyBS.startsWith(LiteralsUtils.LANG_OPERATOR)) {
						langObjects.put(keyBS, new SectionElement(start, section));
						langSectionSize += section.getNumberOfElements();
					} else if (!keyBS.equals(LiteralsUtils.NO_DATATYPE)) {
						// we keep this one
						futureAllObjects.put(keyBS, UnmodifiableDictionarySectionPrivate.of(section));
						starts.put(keyBS, start);
					} else {
						continue; // already computed the NDT
					}
					start += section.getNumberOfElements();
				}

				try (WriteDictionarySectionPrivate wObjects = DictionarySectionFactory.createWriteSection(options,
						dir.resolveSibling("objects"), bufferSize)) {
					futureAllObjects.put(LiteralsUtils.LITERAL_LANG_TYPE, wObjects);

					long languageLocation = 0;
					long writeLocation = 1;
					for (var e : futureAllObjects.entrySet()) {
						ByteString key = e.getKey();
						DictionarySectionPrivate section = e.getValue();

						if (key == LiteralsUtils.LITERAL_LANG_TYPE) {
							languageLocation = writeLocation;
							writeLocation += langSectionSize;
						} else {
							long startValue = starts.get(key);

							for (long j = 0; j < section.getNumberOfElements(); j++) {
								objectMap.set(startValue + j, writeLocation++);
							}
						}
					}

					MSDLSectionLangMerger merger = new MSDLSectionLangMerger(langObjects, objectMap, languageLocation);
					// load the new objects
					wObjects.load(merger, listener);

					try (WriteBitmapTriples triples = new WriteBitmapTriples(options, dir.resolve("triples"),
							bufferSize)) {
						triples.load(new OneReadTempTriples(
								new ObjectReSortIterator(new MapIterator<>(origin.getTriples().searchAll(), tid -> {
									if (tid.getObject() <= nShared) {
										return tid;
									}
									assert objectMap.get(tid.getObject() - nShared) != 0
											: "bad index " + (tid.getObject() - nShared) + "/" + nShared;
									return new TripleID(tid.getSubject(), tid.getPredicate(),
											objectMap.get(tid.getObject() - nShared) + nShared);
								}), order), order, origin.getTriples().getNumberOfElements()), listener);
						// HEADER
						HeaderPrivate header = new PlainHeader();

						Dictionary od = origin.getDictionary();
						// we don't need to copy the subjects/predcates/shared
						// elements because only the object IDs are updated
						DictionaryPrivate dictionary = new MultipleSectionDictionary(options,
								UnmodifiableDictionarySectionPrivate.of(od.getSubjects()),
								UnmodifiableDictionarySectionPrivate.of(od.getPredicates()), futureAllObjects,
								UnmodifiableDictionarySectionPrivate.of(od.getShared()));

						HDTImpl hdt = new HDTImpl(header, dictionary, triples, options);
						hdt.populateHeaderStructure(origin.getBaseURI());
						long rawSize = HDTBase.getRawSize(origin.getHeader());
						if (rawSize != -1) {
							hdt.getHeader().insert("_:statistics", HDTVocabulary.ORIGINAL_SIZE, rawSize);
						}
						hdt.saveToHDT(destination, listener);
						// no need to close the HDT, all the components are
						// already closed after
					}
				}
			}
		}
	}

	static class MSDLSectionLangMerger implements TempDictionarySection {
		private final Map<? extends CharSequence, SectionElement> objects;
		private final DynamicSequence objectMap;
		private final long ndtLocation;

		MSDLSectionLangMerger(Map<? extends CharSequence, SectionElement> objects, DynamicSequence objectMap,
				long ndtLocation) {
			this.objects = objects;
			this.objectMap = objectMap;
			this.ndtLocation = ndtLocation;
		}

		@Override
		public long locate(CharSequence s) {
			throw new NotImplementedException();
		}

		@Override
		public CharSequence extract(long pos) {
			throw new NotImplementedException();
		}

		@Override
		public long size() {
			return objects.values().stream().mapToLong(SectionElement::size).sum();
		}

		@Override
		public long getNumberOfElements() {
			return objects.values().stream().mapToLong(SectionElement::getNumberOfElements).sum();
		}

		@Override
		public Iterator<? extends CharSequence> getSortedEntries() {
			Set<? extends Map.Entry<? extends CharSequence, SectionElement>> set = objects.entrySet();
			List<ExceptionIterator<IndexedNode, RuntimeException>> iterators = new ArrayList<>(set.size());
			for (Map.Entry<? extends CharSequence, SectionElement> e : set) {

				ByteString type = ByteString.of(e.getKey());
				SectionElement section = e.getValue();

				final long currentShift = section.start;
				ExceptionIterator<? extends CharSequence, RuntimeException> it = ExceptionIterator
						.of(section.section.getSortedEntries());
				iterators.add(it.map((s, index) -> new IndexedNode(type.copyPreAppend(s), index + currentShift)));
			}

			ExceptionIterator<IndexedNode, RuntimeException> merge = MergeExceptionIterator.buildOfTree(iterators,
					IndexedNode::compareTo);
			return merge.map((in, futureIndex) -> {
				assert objectMap.get(in.getIndex()) == 0 : "id " + in.getIndex();
				objectMap.set(in.getIndex(), futureIndex + ndtLocation);
				return in.getNode();
			}).asIterator();
		}

		@Override
		public void close() {
			// closed by the HDT owner, nothing to do
		}

		@Override
		public long add(CharSequence str) {
			throw new NotImplementedException();
		}

		@Override
		public void remove(CharSequence str) {
			throw new NotImplementedException();
		}

		@Override
		public void sort() {
			// already sorted
		}

		@Override
		public void clear() {
			throw new NotImplementedException();
		}

		@Override
		public boolean isSorted() {
			return true;
		}

		@Override
		public Iterator<? extends CharSequence> getEntries() {
			return getSortedEntries();
		}
	}

	private record SectionElement(long start, DictionarySection section) {
		long size() {
			return section.size();
		}

		long getNumberOfElements() {
			return section.getNumberOfElements();
		}
	}
}
