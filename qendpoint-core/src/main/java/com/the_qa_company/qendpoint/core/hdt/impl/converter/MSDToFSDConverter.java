package com.the_qa_company.qendpoint.core.hdt.impl.converter;

import com.the_qa_company.qendpoint.core.compact.sequence.DynamicSequence;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64BigDisk;
import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.dictionary.DictionaryPrivate;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.impl.FourSectionDictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.UnmodifiableDictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.WriteDictionarySection;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.hdt.Converter;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MSDToFSDConverter implements Converter {
	@Override
	public String getDestinationType() {
		return HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_SECTION;
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

		if (!HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION.equals(origin.getDictionary().getType())) {
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
				Map<? extends CharSequence, DictionarySection> objects = origin.getDictionary().getAllObjects();
				MSDSectionMerger merger = new MSDSectionMerger(objects, objectMap);
				objectMap.clear();
				try (WriteDictionarySection wObjects = new WriteDictionarySection(options,
						dir.resolveSibling("objects"), bufferSize)) {
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
						DictionaryPrivate dictionary = new FourSectionDictionary(options,
								UnmodifiableDictionarySectionPrivate.of(od.getSubjects()),
								UnmodifiableDictionarySectionPrivate.of(od.getPredicates()), wObjects,
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

	static class MSDSectionMerger implements TempDictionarySection {
		private final Map<? extends CharSequence, DictionarySection> objects;
		private final DynamicSequence objectMap;

		MSDSectionMerger(Map<? extends CharSequence, DictionarySection> objects, DynamicSequence objectMap) {
			this.objects = objects;
			this.objectMap = objectMap;
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
			return objects.values().stream().mapToLong(DictionarySection::size).sum();
		}

		@Override
		public long getNumberOfElements() {
			return objects.values().stream().mapToLong(DictionarySection::getNumberOfElements).sum();
		}

		@Override
		public Iterator<? extends CharSequence> getSortedEntries() {
			Set<? extends Map.Entry<? extends CharSequence, DictionarySection>> set = objects.entrySet();
			List<ExceptionIterator<IndexedNode, RuntimeException>> iterators = new ArrayList<>(set.size());
			long shift = 1;
			for (Map.Entry<? extends CharSequence, DictionarySection> e : set) {

				ByteString type = ByteString.of(e.getKey());
				DictionarySection section = e.getValue();

				final long currentShift = shift;
				ExceptionIterator<? extends CharSequence, RuntimeException> it = ExceptionIterator
						.of(section.getSortedEntries());
				if (LiteralsUtils.NO_DATATYPE.equals(type) || LiteralsUtils.LITERAL_LANG_TYPE.equals(type)) {
					// no datatype, we don't need to add anything
					iterators.add(it.map((s, index) -> new IndexedNode(ByteString.of(s), index + currentShift)));
				} else {
					final ByteString operType = type.copyPreAppend("^^");
					iterators.add(
							it.map((s, index) -> new IndexedNode(operType.copyPreAppend(s), index + currentShift)));
				}
				shift += section.getNumberOfElements();
			}

			ExceptionIterator<IndexedNode, RuntimeException> merge = MergeExceptionIterator.buildOfTree(iterators,
					IndexedNode::compareTo);
			return merge.map((in, futureIndex) -> {
				assert objectMap.get(in.getIndex()) == 0 : "id " + in.getIndex();
				objectMap.set(in.getIndex(), futureIndex + 1);
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
}
