package com.the_qa_company.qendpoint.core.hdt.impl.converter;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.compact.sequence.DynamicSequence;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64BigDisk;
import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.dictionary.DictionaryPrivate;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.WriteDictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.WriteDictionarySectionPrivateAppender;
import com.the_qa_company.qendpoint.core.dictionary.impl.MultipleSectionDictionaryLang;
import com.the_qa_company.qendpoint.core.dictionary.impl.UnmodifiableDictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.DictionarySectionFactory;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.hdt.Converter;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.hdt.impl.HDTBase;
import com.the_qa_company.qendpoint.core.hdt.impl.HDTImpl;
import com.the_qa_company.qendpoint.core.header.HeaderPrivate;
import com.the_qa_company.qendpoint.core.header.PlainHeader;
import com.the_qa_company.qendpoint.core.iterator.utils.MapIterator;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.TriplesFactory;
import com.the_qa_company.qendpoint.core.triples.TriplesPrivate;
import com.the_qa_company.qendpoint.core.triples.impl.OneReadTempTriples;
import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.ContainerException;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class FSDToMSDLConverter implements Converter {
	private static final long[] BUCKET_LONG_CHECK = { 'H', 'D', 'T' };

	@Override
	public String getDestinationType() {
		return HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG;
	}

	@Override
	public void convertHDTFile(HDT origin, Path destination, ProgressListener listener, HDTOptions options)
			throws IOException {
		// remove null
		listener = ProgressListener.ofNullable(listener);
		options = HDTOptions.ofNullable(options).pushTop();

		if (!(origin.getTriples() instanceof TriplesPrivate tp)) {
			throw new IllegalArgumentException("Can't convert triples not implementing the TriplesPrivate interface");
		}
		TripleComponentOrder order = tp.getOrder();

		if (order.getSubjectMapping() == null || order.getSubjectMapping() == TripleComponentRole.OBJECT) {
			throw new IllegalArgumentException("Can't convert triples with order setting the objects in the subjects");
		}

		int bufferSize = options.getInt32(HDTOptionsKeys.LOADER_DISK_BUFFER_SIZE_KEY, CloseSuppressPath.BUFFER_SIZE);

		if (!HDTVocabulary.DICTIONARY_TYPE_FOUR_SECTION.equals(origin.getDictionary().getType())) {
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

			long nShared = origin.getDictionary().getNshared();
			long nObjects = origin.getDictionary().getNobjects() - nShared;
			try (SequenceLog64BigDisk objectMap = new SequenceLog64BigDisk(dir.resolve("objectMap"),
					BitUtil.log2(nObjects), nObjects + 1)) {
				try (FSDSectionBucketFiller buckets = new FSDSectionBucketFiller(options, dir.resolve("buckets"),
						objectMap, bufferSize)) {
					objectMap.clear();
					// load the new objects
					buckets.load(origin.getDictionary().getObjects().getSortedEntries(),
							origin.getDictionary().getNobjects(), listener);

					try (TriplesPrivate triples = TriplesFactory.createWriteTriples(options, dir.resolve("triples"),
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
								}), order), order, origin.getTriples().getNumberOfElements(), 0, nShared), listener);
						// HEADER
						HeaderPrivate header = new PlainHeader();

						Dictionary od = origin.getDictionary();
						// we don't need to copy the subjects/predicates/shared
						// elements because only the object IDs are updated

						TreeMap<ByteString, DictionarySectionPrivate> objects = new TreeMap<>();
						buckets.objects.entrySet().stream().filter(e -> !e.getKey().equals(LiteralsUtils.NO_DATATYPE))
								.forEach(e -> objects.put(e.getKey(), e.getValue()));

						// avoid touching the Write maps
						options.set(HDTOptionsKeys.DICTIONARY_MSDL_NO_RDFTYPE_INDEX, true);
						DictionaryPrivate dictionary = new MultipleSectionDictionaryLang(options,
								UnmodifiableDictionarySectionPrivate.of(od.getSubjects()),
								UnmodifiableDictionarySectionPrivate.of(od.getPredicates()),
								buckets.objects.get(LiteralsUtils.NO_DATATYPE), objects, buckets.languages,
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

	static class FSDSectionBucketFiller implements Closeable {
		private final TreeMap<ByteString, DictionarySectionPrivate> objects = new TreeMap<>();
		private final TreeMap<ByteString, DictionarySectionPrivate> languages = new TreeMap<>();
		private final DynamicSequence objectMap;
		private final CloseSuppressPath dir;
		private final HDTOptions options;
		private final int bufferSize;

		FSDSectionBucketFiller(HDTOptions options, CloseSuppressPath dir, DynamicSequence objectMap, int bufferSize)
				throws IOException {
			this.options = options;
			this.dir = dir;
			dir.mkdirs();
			dir.closeWithDeleteRecurse();
			this.objectMap = objectMap;
			this.bufferSize = bufferSize;
		}

		public void load(Iterator<? extends CharSequence> iterator, long size, ProgressListener listener)
				throws IOException {
			Map<ByteString, Bucket> objectsAppender = new TreeMap<>();
			Map<ByteString, Bucket> languagesAppender = new TreeMap<>();
			long objectIndex = 0;
			try {
				// loading triples
				while (iterator.hasNext()) {
					ByteString str = ByteString.of(iterator.next());
					++objectIndex;
					ByteString type = (ByteString) LiteralsUtils.getType(str);

					try {
						Bucket bucket;

						if (type == LiteralsUtils.LITERAL_LANG_TYPE) {
							ByteString lang = (ByteString) LiteralsUtils.getLanguage(str).orElseThrow();
							bucket = languagesAppender.computeIfAbsent(lang, key -> {
								int id = languages.size();
								WriteDictionarySectionPrivate section = DictionarySectionFactory
										.createWriteSection(options, dir.resolve("lang_" + id + ".sec"), bufferSize);
								languages.put(lang, section);
								try {
									CloseSuppressPath idsPath = dir.resolve("lang_" + id + ".triples");
									return new Bucket(section.createAppender(size, listener), idsPath,
											idsPath.openOutputStream(bufferSize));
								} catch (IOException e) {
									throw new ContainerException(e);
								}
							});
						} else {
							bucket = objectsAppender.computeIfAbsent(type, key -> {
								int id = objects.size();
								WriteDictionarySectionPrivate section = DictionarySectionFactory
										.createWriteSection(options, dir.resolve("type_" + id + ".sec"), bufferSize);
								objects.put(type, section);
								try {
									CloseSuppressPath idsPath = dir.resolve("type_" + id + ".triples");
									return new Bucket(section.createAppender(size, listener), idsPath,
											idsPath.openOutputStream(bufferSize));
								} catch (IOException e) {
									throw new ContainerException(e);
								}
							});
						}
						WriteDictionarySectionPrivateAppender appender = bucket.appender();
						appender.append((ByteString) LiteralsUtils.removeTypeAndLang(str));
						OutputStream ids = bucket.idWriter();
						// write index -> inSectionIndex
						VByte.encode(ids, objectIndex);
					} catch (ContainerException e) {
						throw (IOException) e.getCause();
					}
				}
			} finally {
				Closer.closeAll(objectsAppender, languagesAppender);
			}

			Bucket[] buckets = new Bucket[objectsAppender.size() + languagesAppender.size()];
			Bucket ndt = objectsAppender.get(LiteralsUtils.NO_DATATYPE);
			int index = 0;
			if (ndt != null) {
				buckets[index++] = ndt;
			}
			for (var e : objectsAppender.entrySet()) {
				if (LiteralsUtils.NO_DATATYPE.equals(e.getKey())) {
					continue; // ignore this section
				}
				buckets[index++] = e.getValue();
			}
			for (var e : languagesAppender.entrySet()) {
				buckets[index++] = e.getValue();
			}

			long currentShift = 1;
			for (Bucket bucket : buckets) {
				if (bucket == null) {
					continue;
				}
				try (InputStream idsStream = bucket.idsPath().openInputStream(bufferSize)) {
					long id;

					while ((id = VByte.decode(idsStream)) != 0) {
						assert objectMap.get(id) == 0 : "redefining object: " + id;
						objectMap.set(id, currentShift++);
					}
					for (long l : BUCKET_LONG_CHECK) {
						long r;
						if ((r = VByte.decode(idsStream)) != l) {
							throw new IOException("Check isn't the same as intended " + r + " != " + l);
						}
					}
				}
			}
		}

		@Override
		public void close() throws IOException {
			Closer.closeAll(objects, languages);
		}

	}

	private record Bucket(WriteDictionarySectionPrivateAppender appender, CloseSuppressPath idsPath,
			OutputStream idWriter) implements Closeable {

		@Override
		public void close() throws IOException {
			try {
				VByte.encode(idWriter, 0);
				for (long l : BUCKET_LONG_CHECK) {
					VByte.encode(idWriter, l);
				}
			} finally {
				IOUtil.closeAll(appender, idWriter);
			}
		}
	}
}
