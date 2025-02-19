package com.the_qa_company.qendpoint.core.hdt.impl.converter;

import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.dictionary.DictionaryPrivate;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.impl.WriteMultipleSectionDictionaryLangPrefixes;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.WriteDictionarySection;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
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
import com.the_qa_company.qendpoint.core.triples.TriplesPrivate;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.PrefixesStorage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.TreeMap;

public class MSDLToMSDLPConverter implements Converter {
	@Override
	public String getDestinationType() {
		return HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG_PREFIXES;
	}

	@Override
	public void convertHDTFile(HDT origin, Path destination, ProgressListener listener, HDTOptions _options)
			throws IOException {
		listener = ProgressListener.ofNullable(listener);
		HDTOptions options = HDTOptions.ofNullable(_options);

		PrefixesStorage storage = new PrefixesStorage();
		storage.loadConfig(options.get(HDTOptionsKeys.LOADER_PREFIXES));

		if (!(origin.getTriples() instanceof TriplesPrivate)) {
			throw new IllegalArgumentException("Can't convert triples not implementing the TriplesPrivate interface");
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

			// building one sec by one sec

			HeaderPrivate header = new PlainHeader();

			Dictionary od = origin.getDictionary();

			TreeMap<ByteString, DictionarySectionPrivate> tm = new TreeMap<>();
			Path remapWork = wipDir.resolve("remap");
			od.getAllObjects().forEach(
					(k, s) -> tm.put(ByteString.of(k), new RemapDictionarySection(s, remapWork, storage, options)));
			try (DictionaryPrivate dictionary = new WriteMultipleSectionDictionaryLangPrefixes(options,
					new RemapDictionarySection(od.getSubjects(), remapWork, storage, options),
					new RemapDictionarySection(od.getPredicates(), remapWork, storage, options),
					new RemapDictionarySection(od.getShared(), remapWork, storage, options), tm, storage)) {
				HDTImpl hdt = new HDTImpl(header, dictionary, (TriplesPrivate) origin.getTriples(), options);
				hdt.populateHeaderStructure(origin.getBaseURI());
				long rawSize = HDTBase.getRawSize(origin.getHeader());
				if (rawSize != -1) {
					hdt.getHeader().insert("_:statistics", HDTVocabulary.ORIGINAL_SIZE, rawSize);
				}
				hdt.saveToHDT(destination, listener);
			}
		}
	}

	private static class RemapDictionarySection implements DictionarySectionPrivate {

		private final DictionarySectionPrivate original;
		private final Path dataDir;
		private final PrefixesStorage prefixesStorage;
		private final HDTOptions options;

		private RemapDictionarySection(DictionarySection original, Path dataDir, PrefixesStorage prefixesStorage,
				HDTOptions options) {
			this.original = (DictionarySectionPrivate) original;
			this.dataDir = dataDir;
			this.prefixesStorage = prefixesStorage;
			this.options = options;
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
			return original.size();
		}

		@Override
		public long getNumberOfElements() {
			return original.getNumberOfElements();
		}

		@Override
		public Iterator<? extends CharSequence> getSortedEntries() {
			throw new NotImplementedException();
		}

		@Override
		public void load(TempDictionarySection other, ProgressListener listener) {
			throw new NotImplementedException();
		}

		@Override
		public void load(Iterator<? extends CharSequence> it, long count, ProgressListener listener) {
			throw new NotImplementedException();
		}

		@Override
		public void save(OutputStream output, ProgressListener listener) throws IOException {
			if (prefixesStorage.empty()) {
				original.save(output, listener);
				return; // nothing to do
			}

			// we need to compute the data and the offsets

			Files.createDirectories(dataDir);
			Path ws = dataDir.resolve("sec.bin");
			try (WriteDictionarySection sec = new WriteDictionarySection(options, ws, 4096)) {

				Iterator<? extends CharSequence> mapped = original.getSortedEntries();

				mapped = MapIterator.of(mapped, (s) -> LiteralsUtils.resToPrefLangCut(s, prefixesStorage));

				sec.load(mapped, original.getNumberOfElements(), listener);

				sec.save(output, listener);
			}
		}

		@Override
		public void load(InputStream input, ProgressListener listener) {
			throw new NotImplementedException();
		}

		@Override
		public void close() throws IOException {
			IOUtil.deleteDirRecurse(dataDir);
		}
	}
}
