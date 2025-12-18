package com.the_qa_company.qendpoint.core.hdt.impl.converter;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.compact.sequence.DynamicSequence;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64BigDisk;
import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.dictionary.DictionaryPrivate;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.WriteDictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.WriteDictionarySectionPrivateAppender;
import com.the_qa_company.qendpoint.core.dictionary.impl.MultipleSectionDictionaryLang;
import com.the_qa_company.qendpoint.core.dictionary.impl.UnmodifiableDictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.DictionarySectionFactory;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.PFCDictionarySection;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class MSDToMSDLConverter implements Converter {
	private static final long[] BUCKET_LONG_CHECK = { 'H', 'D', 'L' };

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

			long nShared = origin.getDictionary().getNshared();
			long nObjects = origin.getDictionary().getNobjects() - nShared;
			try (SequenceLog64BigDisk objectMap = new SequenceLog64BigDisk(dir.resolve("objectMap"),
					BitUtil.log2(nObjects), nObjects + 1)) {
				objectMap.clear();
				Map<? extends CharSequence, DictionarySection> allObjects = origin.getDictionary().getAllObjects();

				DictionarySection ndt = allObjects.getOrDefault(LiteralsUtils.NO_DATATYPE,
						new PFCDictionarySection(options));
				DictionarySection lg = allObjects.get(LiteralsUtils.LITERAL_LANG_TYPE);
				TreeMap<ByteString, DictionarySectionPrivate> typed = new TreeMap<>();

				long start = 1;
				long nonLgLocation = 1 + ndt.getNumberOfElements();
				long lgLocation = 0;
				for (var e : allObjects.entrySet()) {
					ByteString type = ByteString.of(e.getKey());
					DictionarySection section = e.getValue();

					if (lg == section) {
						lgLocation = start;
						start += section.getNumberOfElements();
						continue; // ignore
					}

					if (ndt == section) {
						for (long j = 1; j <= section.getNumberOfElements(); j++) {
							objectMap.set(start++, j);
						}
						continue;
					}

					for (long j = 0; j < section.getNumberOfElements(); j++) {
						objectMap.set(start++, nonLgLocation++);
					}

					typed.put(type, UnmodifiableDictionarySectionPrivate.of(section));
				}

				try (MSDSectionBucketFiller buckets = new MSDSectionBucketFiller(options, dir.resolve("buckets"),
						objectMap, bufferSize, lgLocation, nonLgLocation)) {

					// load the new objects
					if (lg != null) {
						buckets.load(lg.getSortedEntries(), lg.getNumberOfElements(), listener);
					}

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
						// avoid touching the Write maps
						options.set(HDTOptionsKeys.DICTIONARY_MSDL_NO_RDFTYPE_INDEX, true);
						DictionaryPrivate dictionary = new MultipleSectionDictionaryLang(options,
								UnmodifiableDictionarySectionPrivate.of(od.getSubjects()),
								UnmodifiableDictionarySectionPrivate.of(od.getPredicates()),
								UnmodifiableDictionarySectionPrivate.of(ndt), typed, buckets.languages,
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

	static class MSDSectionBucketFiller implements Closeable {
		private final TreeMap<ByteString, DictionarySectionPrivate> languages = new TreeMap<>();
		private final DynamicSequence objectMap;
		private final CloseSuppressPath dir;
		private final HDTOptions options;
		private final int bufferSize;

		private final long startOrigin;
		private final long startDest;

		MSDSectionBucketFiller(HDTOptions options, CloseSuppressPath dir, DynamicSequence objectMap, int bufferSize,
				long startOrigin, long startDest) throws IOException {
			this.options = options;
			this.dir = dir;
			dir.mkdirs();
			dir.closeWithDeleteRecurse();
			this.objectMap = objectMap;
			this.bufferSize = bufferSize;
			this.startOrigin = startOrigin;
			this.startDest = startDest;
		}

		public void load(Iterator<? extends CharSequence> iterator, long size, ProgressListener listener)
				throws IOException {
			Map<ByteString, Bucket> languageAppenders = new HashMap<>();
			long objectIndex = startOrigin;
			try {
				// loading triples
				while (iterator.hasNext()) {
					ByteString str = ByteString.of(iterator.next());
					ByteString lang = (ByteString) LiteralsUtils.getLanguage(str).orElseThrow();

					try {
						Bucket bucket = languageAppenders.computeIfAbsent(lang, key -> {
							int id = languages.size();
							WriteDictionarySectionPrivate section = DictionarySectionFactory.createWriteSection(options,
									dir.resolve("lang_" + id + ".sec"), bufferSize);
							languages.put(lang, section);
							try {
								CloseSuppressPath idsPath = dir.resolve("lang_" + id + ".triples");
								return new Bucket(section.createAppender(size, listener), idsPath,
										idsPath.openOutputStream(bufferSize));
							} catch (IOException e) {
								throw new ContainerException(e);
							}
						});
						WriteDictionarySectionPrivateAppender appender = bucket.appender();
						appender.append((ByteString) LiteralsUtils.removeTypeAndLang(str));
						OutputStream ids = bucket.idWriter();
						// write index -> inSectionIndex
						VByte.encode(ids, objectIndex++);
					} catch (ContainerException e) {
						throw (IOException) e.getCause();
					}
				}
			} finally {
				Closer.closeAll(languageAppenders.values());
			}

			long currentShift = startDest;
			for (ByteString type : languages.keySet()) {
				Bucket bucket = languageAppenders.get(type);
				assert bucket != null : "bucket not found for type " + type;
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
			Closer.closeAll(languages);
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
