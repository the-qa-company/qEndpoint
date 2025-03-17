/**
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/hdt/impl/HDTImpl.java
 * $ Revision: $Rev: 202 $ Last modified: $Date: 2013-05-10 18:04:41 +0100 (vie,
 * 10 may 2013) $ Last modified by: $Author: mario.arias $ This library is free
 * software; you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation;
 * version 3.0 of the License. This library is distributed in the hope that it
 * will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 * of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
 * General Public License for more details. You should have received a copy of
 * the GNU Lesser General Public License along with this library; if not, write
 * to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston,
 * MA 02110-1301 USA Contacting the authors: Mario Arias: mario.arias@deri.org
 * Javier D. Fernandez: jfergar@infor.uva.es Miguel A. Martinez-Prieto:
 * migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com Dennis
 * Diefenbach: dennis.diefenbach@univ-st-etienne.fr
 */

package com.the_qa_company.qendpoint.core.hdt.impl;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.compact.bitmap.BitmapFactory;
import com.the_qa_company.qendpoint.core.compact.bitmap.ModifiableBitmap;
import com.the_qa_company.qendpoint.core.dictionary.DictionaryCat;
import com.the_qa_company.qendpoint.core.dictionary.DictionaryDiff;
import com.the_qa_company.qendpoint.core.dictionary.DictionaryFactory;
import com.the_qa_company.qendpoint.core.dictionary.DictionaryPrivate;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.FourSectionDictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.FourSectionDictionaryBig;
import com.the_qa_company.qendpoint.core.dictionary.impl.FourSectionDictionaryCat;
import com.the_qa_company.qendpoint.core.dictionary.impl.MultipleSectionDictionaryBig;
import com.the_qa_company.qendpoint.core.dictionary.impl.MultipleSectionDictionaryCat;
import com.the_qa_company.qendpoint.core.enums.ResultEstimationType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTVersion;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.hdt.TempHDT;
import com.the_qa_company.qendpoint.core.header.HeaderFactory;
import com.the_qa_company.qendpoint.core.header.HeaderPrivate;
import com.the_qa_company.qendpoint.core.iterator.DictionaryTranslateIterator;
import com.the_qa_company.qendpoint.core.iterator.DictionaryTranslateIteratorBuffer;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.ControlInfo;
import com.the_qa_company.qendpoint.core.options.ControlInformation;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.options.HDTSpecification;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import com.the_qa_company.qendpoint.core.triples.DictionaryEntriesDiff;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import com.the_qa_company.qendpoint.core.triples.TempTriples;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.triples.TriplesFactory;
import com.the_qa_company.qendpoint.core.triples.TriplesPrivate;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriples;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriplesCat;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriplesIteratorCat;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriplesIteratorDiff;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriplesIteratorMapDiff;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import com.the_qa_company.qendpoint.core.util.Profiler;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import com.the_qa_company.qendpoint.core.util.io.CountInputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.listener.IntermediateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Basic implementation of HDT interface
 */
public class HDTImpl extends HDTBase<HeaderPrivate, DictionaryPrivate, TriplesPrivate> {
	private static final Logger log = LoggerFactory.getLogger(HDTImpl.class);

	private String hdtFileName;
	private String baseUri;
	private boolean isMapped;
	private boolean isClosed = false;

	public HDTImpl(HDTOptions spec) throws IOException {
		super(spec);

		header = HeaderFactory.createHeader(this.spec);
		dictionary = DictionaryFactory.createDictionary(this.spec);
		triples = TriplesFactory.createTriples(this.spec);
	}

	public HDTImpl(HeaderPrivate header, DictionaryPrivate dictionary, TriplesPrivate triples, HDTOptions spec) {
		super(spec);
		this.header = header;
		this.dictionary = dictionary;
		this.triples = triples;
	}

	@Override
	public void loadFromHDT(InputStream input, ProgressListener listener) throws IOException {
		ControlInfo ci = new ControlInformation();
		IntermediateListener iListener = new IntermediateListener(listener);

		// Load Global ControlInformation
		ci.clear();
		ci.load(input);
		String hdtFormat = ci.getFormat();
		if (!hdtFormat.equals(HDTVocabulary.HDT_CONTAINER) && !hdtFormat.equals(HDTVocabulary.HDT_CONTAINER_2)) {
			throw new IllegalFormatException("This software (v" + HDTVersion.HDT_VERSION + ".x.x | v"
					+ HDTVersion.HDT_VERSION_2 + ".x.x) cannot open this version of HDT File (" + hdtFormat + ")");
		}

		// Load header
		ci.clear();
		ci.load(input);
		iListener.setRange(0, 5);
		header = HeaderFactory.createHeader(ci);
		header.load(input, ci, iListener);

		// Set base URI.
		this.baseUri = header.getBaseURI().toString();

		// Load dictionary
		ci.clear();
		ci.load(input);
		iListener.setRange(5, 60);
		dictionary = DictionaryFactory.createDictionary(ci);
		dictionary.load(input, ci, iListener);

		// Load Triples
		ci.clear();
		ci.load(input);
		iListener.setRange(60, 100);
		triples = TriplesFactory.createTriples(ci);
		triples.load(input, ci, iListener);

		isClosed = false;
	}

	@Override
	public void loadFromHDT(String hdtFileName, ProgressListener listener) throws IOException {
		InputStream in;
		if (hdtFileName.endsWith(".gz")) {
			in = new FastBufferedInputStream(new GZIPInputStream(new FileInputStream(hdtFileName)));
		} else {
			in = new CountInputStream(new FastBufferedInputStream(new FileInputStream(hdtFileName)));
		}
		loadFromHDT(in, listener);
		in.close();

		this.hdtFileName = hdtFileName;

		isClosed = false;
	}

	@Override
	public void mapFromHDT(File f, long offset, ProgressListener listener) throws IOException {
		this.hdtFileName = f.toString();
		this.isMapped = true;

		if (hdtFileName.endsWith(".gz")) {
			File old = f;
			hdtFileName = hdtFileName.substring(0, hdtFileName.length() - 3);
			f = new File(hdtFileName);

			if (!f.exists()) {
				log.warn("We cannot map a gzipped HDT, decompressing into {} first.", hdtFileName);
				IOUtil.decompressGzip(old, f);
				log.warn("Gzipped HDT successfully decompressed. You might want to delete {} to save disk space.", old);
			} else {
				log.error("We cannot map a gzipped HDT, using {} instead.", hdtFileName);
			}
		}

		boolean dumpBinInfo = spec.getBoolean(HDTOptionsKeys.DUMP_BINARY_OFFSETS, false);
		try (CountInputStream input = new CountInputStream(
				new FastBufferedInputStream(new FileInputStream(hdtFileName)), dumpBinInfo)) {

			input.printIndex("HDT CI");

			ControlInfo ci = new ControlInformation();
			IntermediateListener iListener = new IntermediateListener(listener);

			// Load Global ControlInformation
			ci.clear();
			ci.load(input);
			String hdtFormat = ci.getFormat();
			if (!hdtFormat.equals(HDTVocabulary.HDT_CONTAINER) && !hdtFormat.equals(HDTVocabulary.HDT_CONTAINER_2)) {
				throw new IllegalFormatException("This software (v" + HDTVersion.HDT_VERSION + ".x.x | v"
						+ HDTVersion.HDT_VERSION_2 + ".x.x) cannot open this version of HDT File hdtFileName:"
						+ hdtFileName + " format:" + hdtFormat + "");
			}

			input.printIndex("HDT Header");

			// Load header
			ci.clear();
			ci.load(input);
			iListener.setRange(0, 5);
			header = HeaderFactory.createHeader(ci);
			header.load(input, ci, iListener);

			// Set base URI.
			this.baseUri = header.getBaseURI().toString();
			if (baseUri.isEmpty()) {
				log.error("Empty base uri!");
			}

			input.printIndex("HDT Dictionary");

			// Load dictionary
			ci.clear();
			input.mark(1024);
			ci.load(input);
			input.reset();
			iListener.setRange(5, 60);
			dictionary = DictionaryFactory.createDictionary(ci);
			dictionary.mapFromFile(input, f, iListener);

			// Load Triples
			ci.clear();
			input.mark(1024);
			ci.load(input);
			input.reset();
			iListener.setRange(60, 100);
			input.printIndex("HDT Triples");
			triples = TriplesFactory.createTriples(ci);
			triples.mapFromFile(input, f, iListener);
		}

		isClosed = false;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.HDT#saveToHDT(java.io.OutputStream)
	 */
	@Override
	public void saveToHDT(String fileName, ProgressListener listener) throws IOException {
		try (OutputStream out = new FastBufferedOutputStream(new FileOutputStream(fileName))) {
			// OutputStream out = new GZIPOutputStream(new
			// BufferedOutputStream(new FileOutputStream(fileName)));
			saveToHDT(out, listener);
		}

		this.hdtFileName = fileName;
	}

	@Override
	public IteratorTripleString search(CharSequence subject, CharSequence predicate, CharSequence object)
			throws NotFoundException {
		return search(subject, predicate, object, TripleComponentOrder.ALL_MASK);
	}

	@Override
	public IteratorTripleString search(CharSequence subject, CharSequence predicate, CharSequence object,
			CharSequence graph) throws NotFoundException {
		return search(subject, predicate, object, graph, TripleComponentOrder.ALL_MASK);
	}

	@Override
	public IteratorTripleString search(CharSequence subject, CharSequence predicate, CharSequence object,
			int searchOrderMask) throws NotFoundException {

		if (isClosed) {
			throw new IllegalStateException("Cannot search an already closed HDT");
		}

		// Conversion from TripleString to TripleID
		TripleID triple;

		if (dictionary.supportGraphs()) {
			triple = new TripleID(dictionary.stringToId(subject, TripleComponentRole.SUBJECT),
					dictionary.stringToId(predicate, TripleComponentRole.PREDICATE),
					dictionary.stringToId(object, TripleComponentRole.OBJECT), 0);
		} else {
			triple = new TripleID(dictionary.stringToId(subject, TripleComponentRole.SUBJECT),
					dictionary.stringToId(predicate, TripleComponentRole.PREDICATE),
					dictionary.stringToId(object, TripleComponentRole.OBJECT));
		}

		if (triple.isNoMatch()) {
			// throw new NotFoundException("String not found in dictionary");
			return new IteratorTripleString() {
				@Override
				public TripleString next() {
					return null;
				}

				@Override
				public boolean hasNext() {
					return false;
				}

				@Override
				public ResultEstimationType numResultEstimation() {
					return ResultEstimationType.EXACT;
				}

				@Override
				public void goToStart() {
				}

				@Override
				public long estimatedNumResults() {
					return 0;
				}

				@Override
				public long getLastTriplePosition() {
					throw new NotImplementedException();
				}

				@Override
				public TripleComponentOrder getOrder() {
					return TripleComponentOrder.getAcceptableOrder(searchOrderMask);
				}

				@Override
				public boolean isLastTriplePositionBoundToOrder() {
					return false;
				}
			};
		}

		CharSequence g = dictionary.supportGraphs() ? "" : null;

		if (isMapped) {
			try {
				return new DictionaryTranslateIteratorBuffer(triples.search(triple, searchOrderMask), dictionary,
						subject, predicate, object, g);
			} catch (NullPointerException e) {
				e.printStackTrace();
				// FIXME: find why this can happen
				return new DictionaryTranslateIterator(triples.search(triple, searchOrderMask), dictionary, subject,
						predicate, object, g);
			}
		} else {
			return new DictionaryTranslateIterator(triples.search(triple, searchOrderMask), dictionary, subject,
					predicate, object, g);
		}
	}

	@Override
	public IteratorTripleString search(CharSequence subject, CharSequence predicate, CharSequence object,
			CharSequence graph, int searchOrderMask) throws NotFoundException {
		if (isClosed) {
			throw new IllegalStateException("Cannot search an already closed HDT");
		}

		if (!dictionary.supportGraphs()) {
			if (graph != null && !graph.isEmpty()) {
				throw new IllegalArgumentException("This dictionary doesn't support graph");
			}
			// fallback to the default implementation
			return search(subject, predicate, object);
		}

		// Conversion from TripleString to TripleID
		TripleID triple = new TripleID(dictionary.stringToId(subject, TripleComponentRole.SUBJECT),
				dictionary.stringToId(predicate, TripleComponentRole.PREDICATE),
				dictionary.stringToId(object, TripleComponentRole.OBJECT),
				dictionary.stringToId(graph, TripleComponentRole.GRAPH));

		if (triple.isNoMatch()) {
			// throw new NotFoundException("String not found in dictionary");
			return new IteratorTripleString() {
				@Override
				public TripleString next() {
					return null;
				}

				@Override
				public boolean hasNext() {
					return false;
				}

				@Override
				public ResultEstimationType numResultEstimation() {
					return ResultEstimationType.EXACT;
				}

				@Override
				public void goToStart() {
				}

				@Override
				public long estimatedNumResults() {
					return 0;
				}

				@Override
				public long getLastTriplePosition() {
					throw new NotImplementedException();
				}

				@Override
				public TripleComponentOrder getOrder() {
					return TripleComponentOrder.getAcceptableOrder(searchOrderMask);
				}

				@Override
				public boolean isLastTriplePositionBoundToOrder() {
					return false;
				}
			};
		}

		if (isMapped) {
			try {
				return new DictionaryTranslateIteratorBuffer(triples.search(triple, searchOrderMask), dictionary,
						subject, predicate, object, graph);
			} catch (NullPointerException e) {
				e.printStackTrace();
				// FIXME: find why this can happen
				return new DictionaryTranslateIterator(triples.search(triple, searchOrderMask), dictionary, subject,
						predicate, object, graph);
			}
		} else {
			return new DictionaryTranslateIterator(triples.search(triple, searchOrderMask), dictionary, subject,
					predicate, object, graph);
		}
	}

	public void loadFromParts(HeaderPrivate h, DictionaryPrivate d, TriplesPrivate t) {
		this.header = h;
		this.dictionary = d;
		this.triples = t;
		isClosed = false;
	}

	@Override
	public void setBaseUri(String baseUri) {
		this.baseUri = baseUri;
	}

	public void loadFromModifiableHDT(TempHDT modHdt, ProgressListener listener) {

		modHdt.reorganizeDictionary(listener);
		modHdt.reorganizeTriples(listener);

		// Get parts
		TempTriples modifiableTriples = modHdt.getTriples();
		TempDictionary modifiableDictionary = modHdt.getDictionary();

		// Convert triples to final format
		if (triples.getClass().equals(modifiableTriples.getClass())) {
			triples = modifiableTriples;
		} else {
			triples.load(modifiableTriples, listener);
		}

		// Convert dictionary to final format
		if (dictionary.getClass().equals(modifiableDictionary.getClass())) {
			dictionary = (DictionaryPrivate) modifiableDictionary;
		} else {
			dictionary.load(modifiableDictionary, listener);
		}

		this.baseUri = modHdt.getBaseURI();
		isClosed = false;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.hdt.HDT#generateIndex(hdt.listener.ProgressListener)
	 */
	@Override
	public void loadOrCreateIndex(ProgressListener listener, HDTOptions spec) throws IOException {
		if (triples.getNumberOfElements() == 0) {
			// We need no index.
			return;
		}
		triples.mapGenOtherIndexes(Path.of(String.valueOf(hdtFileName)), spec, listener);

		// disable the FOQ generation if asked
		if (spec.getBoolean(HDTOptionsKeys.BITMAPTRIPLES_INDEX_NO_FOQ, false)) {
			return;
		}

		ControlInfo ci = new ControlInformation();
		String indexName = hdtFileName + HDTVersion.get_index_suffix("-");
		indexName = indexName.replaceAll("\\.hdt\\.gz", "hdt");
		String versionName = indexName;
		File ff = new File(indexName);
		// backward compatibility
		if (!ff.isFile() || !ff.canRead()) {
			indexName = hdtFileName + (".index");
			indexName = indexName.replaceAll("\\.hdt\\.gz", "hdt");
			ff = new File(indexName);
		}
		CountInputStream in = null;
		try {
			in = new CountInputStream(new FastBufferedInputStream(new FileInputStream(ff)));
			ci.load(in);
			if (isMapped) {
				triples.mapIndex(in, new File(indexName), ci, listener);
			} else {
				triples.loadIndex(in, ci, listener);
			}
		} catch (Exception e) {
			if (!(e instanceof FileNotFoundException)) {
				log.warn("Error reading .hdt.index, generating a new one. ", e);
			}

			// GENERATE
			StopWatch st = new StopWatch();
			triples.generateIndex(listener, spec, dictionary);

			// SAVE
			if (this.hdtFileName != null) {
				OutputStream out = null;
				try {
					out = new FastBufferedOutputStream(new FileOutputStream(versionName));
					ci.clear();
					triples.saveIndex(out, ci, listener);
					out.close();
					log.info("Index generated and saved in {}", st.stopAndShow());
				} catch (IOException e2) {
					log.error("Error writing index file.", e2);
				} finally {
					IOUtil.closeQuietly(out);
				}
			}
		} finally {
			IOUtil.closeQuietly(in);
		}
	}

	@Override
	public String getBaseURI() {
		return baseUri;
	}

	protected void setTriples(TriplesPrivate triples) {
		this.triples = triples;
	}

	@Override
	public void close() throws IOException {
		if (isClosed) {
			return;
		}
		isClosed = true;
		IOUtil.closeAll(dictionary, triples);
	}

	// For debugging
	@Override
	public String toString() {
		return String.format("HDT[file=%s,#triples=%d]", hdtFileName, triples.getNumberOfElements());
	}

	public String getHDTFileName() {
		if (hdtFileName == null) {
			try {
				hdtFileName = Files.createTempFile("hdt_", ".hdt").toAbsolutePath().toString();
			} catch (IOException e) {
				hdtFileName = "default.hdt";
				log.warn("Can't create default HDT file name, using '{}'", hdtFileName, e);
			}
		}
		return hdtFileName;
	}

	@Override
	public boolean isClosed() {
		return isClosed;
	}

	public boolean isMapped() {
		return isMapped;
	}

	/**
	 * Merges two hdt files hdt1 and hdt2 on disk at location
	 *
	 * @param location catlocation
	 * @param hdt1     hdt1
	 * @param hdt2     hdt2
	 * @param listener listener
	 */
	public void cat(String location, HDT hdt1, HDT hdt2, ProgressListener listener, Profiler profiler)
			throws IOException {
		if (listener != null) {
			listener.notifyProgress(0, "Generating dictionary");
		}
		try (FourSectionDictionaryCat dictionaryCat = new FourSectionDictionaryCat(location)) {
			profiler.pushSection("catdict");
			dictionaryCat.cat(hdt1.getDictionary(), hdt2.getDictionary(), listener);
			ControlInfo ci2 = new ControlInformation();
			// map the generated dictionary
			FourSectionDictionaryBig dictionary;
			try (CountInputStream fis = new CountInputStream(
					new FastBufferedInputStream(new FileInputStream(location + "dictionary")))) {
				dictionary = new FourSectionDictionaryBig(new HDTSpecification());
				fis.mark(1024);
				ci2.load(fis);
				fis.reset();
				dictionary.mapFromFile(fis, new File(location + "dictionary"), null);
			}
			if (this.dictionary != null) {
				this.dictionary.close();
			}
			this.dictionary = dictionary;

			profiler.popSection();
			profiler.pushSection("cattriples");

			if (listener != null) {
				listener.notifyProgress(0, "Generating triples");
			}
			BitmapTriplesIteratorCat it = new BitmapTriplesIteratorCat(hdt1.getTriples(), hdt2.getTriples(),
					dictionaryCat);
			BitmapTriplesCat bitmapTriplesCat = new BitmapTriplesCat(location);
			bitmapTriplesCat.cat(it, listener);
			profiler.popSection();
		}
		profiler.pushSection("Clean and map");
		// Delete the mappings since they are not necessary anymore
		Files.delete(Paths.get(location + "P1"));
		Files.delete(Paths.get(location + "P1" + "Types"));
		Files.delete(Paths.get(location + "P2"));
		Files.delete(Paths.get(location + "P2" + "Types"));
		Files.delete(Paths.get(location + "SH1"));
		Files.delete(Paths.get(location + "SH1" + "Types"));
		Files.delete(Paths.get(location + "SH2"));
		Files.delete(Paths.get(location + "SH2" + "Types"));
		Files.delete(Paths.get(location + "S1"));
		Files.delete(Paths.get(location + "S1" + "Types"));
		Files.delete(Paths.get(location + "S2"));
		Files.delete(Paths.get(location + "S2" + "Types"));
		Files.delete(Paths.get(location + "O1"));
		Files.delete(Paths.get(location + "O1" + "Types"));
		Files.delete(Paths.get(location + "O2"));
		Files.delete(Paths.get(location + "O2" + "Types"));

		// map the triples
		try (CountInputStream fis2 = new CountInputStream(
				new FastBufferedInputStream(new FileInputStream(location + "triples")))) {
			ControlInfo ci2 = new ControlInformation();
			ci2.clear();
			fis2.mark(1024);
			ci2.load(fis2);
			fis2.reset();
			triples = TriplesFactory.createTriples(ci2);
			triples.mapFromFile(fis2, new File(location + "triples"), null);
		}
		Files.delete(Paths.get(location + "mapping_back_1"));
		Files.delete(Paths.get(location + "mapping_back_2"));
		Files.delete(Paths.get(location + "mapping_back_type_1"));
		Files.delete(Paths.get(location + "mapping_back_type_2"));
		if (listener != null) {
			listener.notifyProgress(0, "Generating header");
		}
		this.header = HeaderFactory.createHeader(spec);
		this.populateHeaderStructure(hdt1.getBaseURI());
		long rawSize1 = getRawSize(hdt1.getHeader());
		long rawSize2 = getRawSize(hdt2.getHeader());

		if (rawSize1 != -1 && rawSize2 != -1) {
			getHeader().insert("_:statistics", HDTVocabulary.ORIGINAL_SIZE, String.valueOf(rawSize1 + rawSize2));
		}
		profiler.popSection();
	}

	public void catCustom(String location, HDT hdt1, HDT hdt2, ProgressListener listener, Profiler profiler)
			throws IOException {
		if (listener != null) {
			listener.notifyProgress(0, "Generating dictionary");
		}
		try (DictionaryCat dictionaryCat = new MultipleSectionDictionaryCat(location)) {
			profiler.pushSection("catdict");
			dictionaryCat.cat(hdt1.getDictionary(), hdt2.getDictionary(), listener);

			// map the generated dictionary
			ControlInfo ci2 = new ControlInformation();
			try (CountInputStream fis = new CountInputStream(
					new FastBufferedInputStream(new FileInputStream(location + "dictionary")))) {
				HDTSpecification spec = new HDTSpecification();
				spec.set(HDTOptionsKeys.TEMP_DICTIONARY_IMPL_KEY, HDTOptionsKeys.TEMP_DICTIONARY_IMPL_VALUE_MULT_HASH);
				spec.set(HDTOptionsKeys.DICTIONARY_TYPE_KEY, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS);
				MultipleSectionDictionaryBig dictionary = new MultipleSectionDictionaryBig(spec);
				fis.mark(1024);
				ci2.load(fis);
				fis.reset();
				dictionary.mapFromFile(fis, new File(location + "dictionary"), null);
				this.dictionary = dictionary;
			}
			profiler.popSection();
			profiler.pushSection("cattriples");

			if (listener != null) {
				listener.notifyProgress(0, "Generating triples");
			}
			BitmapTriplesIteratorCat it = new BitmapTriplesIteratorCat(hdt1.getTriples(), hdt2.getTriples(),
					dictionaryCat);
			BitmapTriplesCat bitmapTriplesCat = new BitmapTriplesCat(location);
			bitmapTriplesCat.cat(it, listener);
			profiler.popSection();
		}
		profiler.pushSection("Clean and map");
		// Delete the mappings since they are not necessary anymore
		int countSubSections = 0;
		for (CharSequence datatype : hdt1.getDictionary().getAllObjects().keySet()) {
			String prefix = "sub" + countSubSections;
			if (datatype.equals(LiteralsUtils.NO_DATATYPE)) {
				prefix = datatype.toString();
			}
			Files.delete(Paths.get(location + prefix + "1"));
			Files.delete(Paths.get(location + prefix + "1" + "Types"));
			countSubSections++;
		}
		countSubSections = 0;
		for (CharSequence datatype : hdt2.getDictionary().getAllObjects().keySet()) {
			String prefix = "sub" + countSubSections;
			if (datatype.equals(LiteralsUtils.NO_DATATYPE)) {
				prefix = datatype.toString();
			}
			Files.delete(Paths.get(location + prefix + "2"));
			Files.delete(Paths.get(location + prefix + "2" + "Types"));
			countSubSections++;
		}
		Files.delete(Paths.get(location + "P1"));
		Files.delete(Paths.get(location + "P1" + "Types"));
		Files.delete(Paths.get(location + "P2"));
		Files.delete(Paths.get(location + "P2" + "Types"));
		Files.delete(Paths.get(location + "SH1"));
		Files.delete(Paths.get(location + "SH1" + "Types"));
		Files.delete(Paths.get(location + "SH2"));
		Files.delete(Paths.get(location + "SH2" + "Types"));
		Files.delete(Paths.get(location + "S1"));
		Files.delete(Paths.get(location + "S1" + "Types"));
		Files.delete(Paths.get(location + "S2"));
		Files.delete(Paths.get(location + "S2" + "Types"));
		Files.delete(Paths.get(location + "O1"));
		Files.delete(Paths.get(location + "O1" + "Types"));
		Files.delete(Paths.get(location + "O2"));
		Files.delete(Paths.get(location + "O2" + "Types"));
		// map the triples
		try (CountInputStream fis2 = new CountInputStream(
				new FastBufferedInputStream(new FileInputStream(location + "triples")))) {
			ControlInformation ci2 = new ControlInformation();
			ci2.clear();
			fis2.mark(1024);
			ci2.load(fis2);
			fis2.reset();
			triples = TriplesFactory.createTriples(ci2);
			triples.mapFromFile(fis2, new File(location + "triples"), null);
		}
		Files.delete(Paths.get(location + "mapping_back_1"));
		Files.delete(Paths.get(location + "mapping_back_2"));
		Files.delete(Paths.get(location + "mapping_back_type_1"));
		Files.delete(Paths.get(location + "mapping_back_type_2"));
		if (listener != null) {
			listener.notifyProgress(0, "Generating header");
		}
		this.header = HeaderFactory.createHeader(spec);
		this.populateHeaderStructure(hdt1.getBaseURI());
		long rawSize1 = getRawSize(hdt1.getHeader());
		long rawSize2 = getRawSize(hdt2.getHeader());

		if (rawSize1 != -1 && rawSize2 != -1) {
			getHeader().insert("_:statistics", HDTVocabulary.ORIGINAL_SIZE, String.valueOf(rawSize1 + rawSize2));
		}
		profiler.popSection();
	}

	public void diff(HDT hdt1, HDT hdt2, ProgressListener listener, Profiler profiler) throws IOException {
		ModifiableBitmap bitmap = BitmapFactory.createRWBitmap(hdt1.getTriples().getNumberOfElements());
		BitmapTriplesIteratorDiff iterator = new BitmapTriplesIteratorDiff(hdt1, hdt2, bitmap);
		profiler.pushSection("fill bitmap");
		iterator.fillBitmap();
		profiler.popSection();
		diffBit(getHDTFileName(), hdt1, bitmap, listener, profiler);
	}

	public void diffBit(String location, HDT hdt, Bitmap deleteBitmap, ProgressListener listener, Profiler profiler)
			throws IOException {
		IntermediateListener il = new IntermediateListener(listener);
		log.debug("Generating Dictionary...");
		il.notifyProgress(0, "Generating Dictionary...");
		profiler.pushSection("diffdict");
		IteratorTripleID hdtIterator = hdt.getTriples().searchAll();
		DictionaryEntriesDiff iter = DictionaryEntriesDiff.createForType(hdt.getDictionary(), hdt, deleteBitmap,
				hdtIterator);

		iter.loadBitmaps();

		Map<CharSequence, ModifiableBitmap> bitmaps = iter.getBitmaps();

		try (DictionaryDiff diff = DictionaryFactory.createDictionaryDiff(hdt.getDictionary(), location)) {

			diff.diff(hdt.getDictionary(), bitmaps, listener);
			// map the generated dictionary
			ControlInfo ci2 = new ControlInformation();

			try (CountInputStream fis = new CountInputStream(
					new FastBufferedInputStream(new FileInputStream(location + "dictionary")))) {
				fis.mark(1024);
				ci2.load(fis);
				fis.reset();
				DictionaryPrivate dictionary = DictionaryFactory.createDictionary(ci2);
				dictionary.mapFromFile(fis, new File(location + "dictionary"), null);
				this.dictionary = dictionary;
			}
			profiler.popSection();

			log.debug("Generating Triples...");

			profiler.pushSection("difftriples");

			il.notifyProgress(40, "Generating Triples...");
			// map the triples based on the new dictionary
			BitmapTriplesIteratorMapDiff mapIter = new BitmapTriplesIteratorMapDiff(hdt, deleteBitmap, diff);

			BitmapTriples triples = new BitmapTriples(spec);
			triples.load(mapIter, listener);
			this.triples = triples;
		}
		profiler.popSection();
		profiler.pushSection("Clean and map");

		log.debug("Clear data...");
		il.notifyProgress(80, "Clear data...");
		if (!(hdt.getDictionary() instanceof FourSectionDictionary)) {
			int count = 0;
			for (CharSequence key : dictionary.getAllObjects().keySet()) {
				CharSequence subPrefix = "sub" + count;
				if (key.equals(LiteralsUtils.NO_DATATYPE)) {
					subPrefix = key;
				}
				Files.delete(Paths.get(location + subPrefix));
				Files.delete(Paths.get(location + subPrefix + "Types"));
				count++;
			}
		}

		Files.delete(Paths.get(location + "predicate"));
		Files.delete(Paths.get(location + "predicate" + "Types"));
		Files.delete(Paths.get(location + "subject"));
		Files.delete(Paths.get(location + "subject" + "Types"));
		Files.delete(Paths.get(location + "object"));
		Files.delete(Paths.get(location + "object" + "Types"));
		Files.delete(Paths.get(location + "shared"));
		Files.delete(Paths.get(location + "shared" + "Types"));
		Files.delete(Paths.get(location + "back" + "Types"));
		Files.delete(Paths.get(location + "back"));
		Files.deleteIfExists(Paths.get(location + "P"));
		Files.deleteIfExists(Paths.get(location + "S"));
		Files.deleteIfExists(Paths.get(location + "O"));
		Files.deleteIfExists(Paths.get(location + "SH_S"));
		Files.deleteIfExists(Paths.get(location + "SH_O"));

		log.debug("Set header...");
		il.notifyProgress(90, "Set header...");
		this.header = HeaderFactory.createHeader(spec);

		this.populateHeaderStructure(hdt.getBaseURI());
		log.debug("Diff completed.");
		il.notifyProgress(100, "Diff completed...");
		profiler.popSection();
	}
}
