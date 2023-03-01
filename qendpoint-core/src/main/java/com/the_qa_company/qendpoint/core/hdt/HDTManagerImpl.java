package com.the_qa_company.qendpoint.core.hdt;

import com.the_qa_company.qendpoint.core.enums.CompressionType;
import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.impl.HDTDiskImporter;
import com.the_qa_company.qendpoint.core.hdt.impl.HDTImpl;
import com.the_qa_company.qendpoint.core.hdt.impl.TempHDTImporterOnePass;
import com.the_qa_company.qendpoint.core.hdt.impl.TempHDTImporterTwoPass;
import com.the_qa_company.qendpoint.core.hdt.writer.TripleWriterHDT;
import com.the_qa_company.qendpoint.core.header.HeaderUtil;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.options.HDTSpecification;
import com.the_qa_company.qendpoint.core.rdf.RDFFluxStop;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.rdf.RDFParserFactory;
import com.the_qa_company.qendpoint.core.rdf.TripleWriter;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.Profiler;
import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.dictionary.impl.MultipleSectionDictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.kcat.KCatImpl;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.CatTreeImpl;
import com.the_qa_company.qendpoint.core.iterator.utils.MapIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.PipedCopyIterator;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

public class HDTManagerImpl extends HDTManager {
	private static final Logger logger = LoggerFactory.getLogger(HDTManagerImpl.class);

	@Override
	public HDTOptions doReadOptions(String file) throws IOException {
		return new HDTSpecification(file);
	}

	public static HDT loadOrMapHDT(Path hdtFileName, ProgressListener listener, HDTOptions spec) throws IOException {
		return loadOrMapHDT(hdtFileName.toAbsolutePath().toString(), listener, spec);
	}

	public static HDT loadOrMapHDT(String hdtFileName, ProgressListener listener, HDTOptions spec) throws IOException {
		String loadingMethod = spec.get(HDTOptionsKeys.LOAD_HDT_TYPE_KEY);
		if (loadingMethod == null || loadingMethod.isEmpty()
				|| HDTOptionsKeys.LOAD_HDT_TYPE_VALUE_MAP.equals(loadingMethod)) {
			return mapHDT(hdtFileName, listener, spec);
		}
		if (HDTOptionsKeys.LOAD_HDT_TYPE_VALUE_LOAD.equals(loadingMethod)) {
			return loadHDT(hdtFileName, listener, spec);
		}
		throw new IllegalArgumentException("Bad loading method: " + loadingMethod);
	}

	@Override
	public HDT doLoadHDT(String hdtFileName, ProgressListener listener, HDTOptions spec) throws IOException {
		HDTPrivate hdt = new HDTImpl(spec);
		hdt.loadFromHDT(hdtFileName, listener);
		return hdt;
	}

	@Override
	protected HDT doMapHDT(String hdtFileName, ProgressListener listener, HDTOptions spec) throws IOException {
		HDTPrivate hdt = new HDTImpl(spec);
		hdt.mapFromHDT(new File(hdtFileName), 0, listener);
		return hdt;
	}

	@Override
	public HDT doLoadHDT(InputStream hdtFile, ProgressListener listener, HDTOptions spec) throws IOException {
		HDTPrivate hdt = new HDTImpl(spec);
		hdt.loadFromHDT(hdtFile, listener);
		return hdt;
	}

	@Override
	public HDT doLoadIndexedHDT(String hdtFileName, ProgressListener listener, HDTOptions spec) throws IOException {
		HDTPrivate hdt = new HDTImpl(spec);
		hdt.loadFromHDT(hdtFileName, listener);
		hdt.loadOrCreateIndex(listener, spec);
		return hdt;
	}

	@Override
	public HDT doMapIndexedHDT(String hdtFileName, ProgressListener listener, HDTOptions spec) throws IOException {
		HDTPrivate hdt = new HDTImpl(spec);
		hdt.mapFromHDT(new File(hdtFileName), 0, listener);
		hdt.loadOrCreateIndex(listener, spec);
		return hdt;
	}

	@Override
	public HDT doLoadIndexedHDT(InputStream hdtFile, ProgressListener listener, HDTOptions spec) throws IOException {
		HDTPrivate hdt = new HDTImpl(spec);
		hdt.loadFromHDT(hdtFile, listener);
		hdt.loadOrCreateIndex(listener, spec);
		return hdt;
	}

	@Override
	public HDT doIndexedHDT(HDT hdt, ProgressListener listener) throws IOException {
		((HDTPrivate) hdt).loadOrCreateIndex(listener, HDTOptions.of());
		return hdt;
	}

	private RDFFluxStop readFluxStopOrSizeLimit(HDTOptions spec) {
		// if no config, use default implementation
		return spec.getFluxStop(HDTOptionsKeys.RDF_FLUX_STOP_KEY, () -> {
			// get the chunk size to base the work
			String loaderType = spec.get(HDTOptionsKeys.LOADER_CATTREE_LOADERTYPE_KEY);

			if (!HDTOptionsKeys.LOADER_TYPE_VALUE_DISK.equals(loaderType)) {
				// memory based implementation, we can only store the NT file
				// divide because the memory implementation is using a lot of
				// memory
				return RDFFluxStop.sizeLimit(getMaxChunkSize() / 4);
			}

			// disk based implementation, we only have to reduce the
			// fault-factor of the map files
			long triplesCount = findBestMemoryChunkDiskMapTreeCat();

			double factor = spec.getDouble(HDTOptionsKeys.LOADER_CATTREE_MEMORY_FAULT_FACTOR, 1.4);

			if (factor <= 0) {
				throw new IllegalArgumentException(
						HDTOptionsKeys.LOADER_CATTREE_MEMORY_FAULT_FACTOR + " can't have a negative or 0 value!");
			}

			// create a count limit from the chunk size / factor, set a minimum
			// value for low factor
			return RDFFluxStop.countLimit(Math.max(128, (long) (triplesCount * factor)));
		});
	}

	@Override
	public HDT doGenerateHDT(String rdfFileName, String baseURI, RDFNotation rdfNotation, HDTOptions spec,
			ProgressListener listener) throws IOException, ParserException {
		// choose the importer
		String loaderType = spec.get(HDTOptionsKeys.LOADER_TYPE_KEY);
		TempHDTImporter loader;
		if (HDTOptionsKeys.LOADER_TYPE_VALUE_DISK.equals(loaderType)) {
			return doGenerateHDTDisk(rdfFileName, baseURI, rdfNotation, CompressionType.guess(rdfFileName), spec,
					listener);
		} else if (HDTOptionsKeys.LOADER_TYPE_VALUE_CAT.equals(loaderType)) {
			return doHDTCatTree(readFluxStopOrSizeLimit(spec), HDTSupplier.fromSpec(spec), rdfFileName, baseURI,
					rdfNotation, spec, listener);
		} else if (HDTOptionsKeys.LOADER_TYPE_VALUE_TWO_PASS.equals(loaderType)) {
			loader = new TempHDTImporterTwoPass(spec);
		} else {
			if (loaderType != null && !HDTOptionsKeys.LOADER_TYPE_VALUE_ONE_PASS.equals(loaderType)) {
				logger.warn("Used the option {} with value {}, which isn't recognize, using default value {}",
						HDTOptionsKeys.LOADER_TYPE_KEY, loaderType, HDTOptionsKeys.LOADER_TYPE_VALUE_ONE_PASS);
			}
			loader = new TempHDTImporterOnePass(spec);
		}

		// Create TempHDT
		try (TempHDT modHdt = loader.loadFromRDF(spec, rdfFileName, baseURI, rdfNotation, listener)) {

			// Convert to HDT
			HDTImpl hdt = new HDTImpl(spec);
			hdt.loadFromModifiableHDT(modHdt, listener);
			hdt.populateHeaderStructure(modHdt.getBaseURI());

			// Add file size to Header
			try {
				long originalSize = HeaderUtil.getPropertyLong(modHdt.getHeader(), "_:statistics",
						HDTVocabulary.ORIGINAL_SIZE);
				hdt.getHeader().insert("_:statistics", HDTVocabulary.ORIGINAL_SIZE, originalSize);
			} catch (NotFoundException e) {
				// ignore
			}

			return hdt;
		}
	}

	@Override
	public HDT doGenerateHDT(InputStream fileStream, String baseURI, RDFNotation rdfNotation,
			CompressionType compressionType, HDTOptions hdtFormat, ProgressListener listener) throws IOException {
		// uncompress the stream if required
		fileStream = IOUtil.asUncompressed(fileStream, compressionType);
		// create a parser for this rdf stream
		RDFParserCallback parser = RDFParserFactory.getParserCallback(rdfNotation);
		// read the stream as triples
		try (PipedCopyIterator<TripleString> iterator = RDFParserFactory.readAsIterator(parser, fileStream, baseURI,
				true, rdfNotation)) {
			return doGenerateHDT(iterator, baseURI, hdtFormat, listener);
		}
	}

	@Override
	public HDT doGenerateHDT(Iterator<TripleString> triples, String baseURI, HDTOptions spec, ProgressListener listener)
			throws IOException {
		// choose the importer
		String loaderType = spec.get(HDTOptionsKeys.LOADER_TYPE_KEY);
		TempHDTImporterOnePass loader;
		if (HDTOptionsKeys.LOADER_TYPE_VALUE_DISK.equals(loaderType)) {
			try {
				return doGenerateHDTDisk(triples, baseURI, spec, listener);
			} catch (ParserException e) {
				throw new RuntimeException(e);
			}
		} else if (HDTOptionsKeys.LOADER_TYPE_VALUE_CAT.equals(loaderType)) {
			try {
				return doHDTCatTree(readFluxStopOrSizeLimit(spec), HDTSupplier.fromSpec(spec), triples, baseURI, spec,
						listener);
			} catch (ParserException e) {
				throw new RuntimeException(e);
			}
		} else {
			if (loaderType != null) {
				if (HDTOptionsKeys.LOADER_TYPE_VALUE_TWO_PASS.equals(loaderType)) {
					logger.warn(
							"Used the option {} with value {}, which isn't available for stream generation, using default value {}",
							HDTOptionsKeys.LOADER_TYPE_KEY, loaderType, HDTOptionsKeys.LOADER_TYPE_VALUE_ONE_PASS);
				} else if (!HDTOptionsKeys.LOADER_TYPE_VALUE_ONE_PASS.equals(loaderType)) {
					logger.warn("Used the option {} with value {}, which isn't recognize, using default value {}",
							HDTOptionsKeys.LOADER_TYPE_KEY, loaderType, HDTOptionsKeys.LOADER_TYPE_VALUE_ONE_PASS);
				}
			}
			loader = new TempHDTImporterOnePass(spec);
		}

		// Create TempHDT
		try (TempHDT modHdt = loader.loadFromTriples(spec, triples, baseURI, listener)) {
			// Convert to HDT
			HDTImpl hdt = new HDTImpl(spec);
			hdt.loadFromModifiableHDT(modHdt, listener);
			hdt.populateHeaderStructure(modHdt.getBaseURI());

			// Add file size to Header
			try {
				long originalSize = HeaderUtil.getPropertyLong(modHdt.getHeader(), "_:statistics",
						HDTVocabulary.ORIGINAL_SIZE);
				hdt.getHeader().insert("_:statistics", HDTVocabulary.ORIGINAL_SIZE, originalSize);
			} catch (NotFoundException e) {
				// ignore
			}

			return hdt;
		}
	}

	@Override
	public HDT doGenerateHDTDisk(String rdfFileName, String baseURI, RDFNotation rdfNotation,
			CompressionType compressionType, HDTOptions hdtFormat, ProgressListener listener)
			throws IOException, ParserException {
		// read this file as stream, do not compress to allow the
		// compressionType to be different from the file extension
		try (InputStream stream = IOUtil.getFileInputStream(rdfFileName, false)) {
			return doGenerateHDTDisk(stream, baseURI, rdfNotation, compressionType, hdtFormat, listener);
		}
	}

	@Override
	public HDT doGenerateHDTDisk(InputStream fileStream, String baseURI, RDFNotation rdfNotation,
			CompressionType compressionType, HDTOptions hdtFormat, ProgressListener listener)
			throws IOException, ParserException {
		// uncompress the stream if required
		fileStream = IOUtil.asUncompressed(fileStream, compressionType);
		// create a parser for this rdf stream
		RDFParserCallback parser = RDFParserFactory.getParserCallback(rdfNotation, hdtFormat);
		// read the stream as triples
		try (PipedCopyIterator<TripleString> iterator = RDFParserFactory.readAsIterator(parser, fileStream, baseURI,
				true, rdfNotation)) {
			return doGenerateHDTDisk0(iterator, true, baseURI, hdtFormat, listener);
		}
	}

	/**
	 * @return a theoretical maximum amount of memory the JVM will attempt to
	 *         use
	 */
	static long getMaxChunkSize() {
		Runtime runtime = Runtime.getRuntime();
		return (long) ((runtime.maxMemory() - (runtime.totalMemory() - runtime.freeMemory())) * 0.85);
	}

	private static long findBestMemoryChunkDiskMapTreeCat() {
		Runtime runtime = Runtime.getRuntime();
		long maxRam = (long) ((runtime.maxMemory() - (runtime.totalMemory() - runtime.freeMemory())) * 0.85) / 3;

		int shift = 0;

		while (shift != 63 && (1L << shift) * BitUtil.log2(1L << shift) < maxRam) {
			shift++;
		}

		// it will take at most "shift" bits per triple
		// we divide by 3 for the 3 maps
		return maxRam / shift;
	}

	@Override
	public HDT doGenerateHDTDisk(Iterator<TripleString> iterator, String baseURI, HDTOptions hdtFormat,
			ProgressListener progressListener) throws IOException, ParserException {
		return doGenerateHDTDisk0(iterator, hdtFormat.getBoolean(HDTOptionsKeys.LOADER_DISK_NO_COPY_ITERATOR_KEY),
				baseURI, hdtFormat, progressListener);
	}

	private HDT doGenerateHDTDisk0(Iterator<TripleString> iterator, boolean copyIterator, String baseURI,
			HDTOptions hdtFormat, ProgressListener progressListener) throws IOException, ParserException {
		try (HDTDiskImporter hdtDiskImporter = new HDTDiskImporter(hdtFormat, progressListener, baseURI)) {
			if (copyIterator) {
				return hdtDiskImporter.runAllSteps(iterator);
			} else {
				// create a copy of the triple at loading time to avoid weird
				// behaviors
				return hdtDiskImporter.runAllSteps(new MapIterator<>(iterator, TripleString::tripleToString));
			}
		}
	}

	@Override
	protected TripleWriter doGetHDTWriter(OutputStream out, String baseURI, HDTOptions hdtFormat) {
		return new TripleWriterHDT(baseURI, hdtFormat, out);
	}

	@Override
	protected TripleWriter doGetHDTWriter(String outFile, String baseURI, HDTOptions hdtFormat) throws IOException {
		return new TripleWriterHDT(baseURI, hdtFormat, outFile, false);
	}

	@Override
	public HDT doHDTCat(String location, String hdtFileName1, String hdtFileName2, HDTOptions hdtFormat,
			ProgressListener listener) throws IOException {
		try (HDT hdt1 = loadOrMapHDT(hdtFileName1, listener, hdtFormat);
				HDT hdt2 = loadOrMapHDT(hdtFileName2, listener, hdtFormat)) {
			HDTImpl hdt = new HDTImpl(hdtFormat);
			try (Profiler profiler = Profiler.createOrLoadSubSection("hdtCat", hdtFormat, false)) {
				try {
					if (hdt1.getDictionary() instanceof MultipleSectionDictionary
							&& hdt2.getDictionary() instanceof MultipleSectionDictionary) {
						hdt.catCustom(location, hdt1, hdt2, listener, profiler);
					} else {
						hdt.cat(location, hdt1, hdt2, listener, profiler);
					}
				} finally {
					profiler.stop();
					profiler.writeProfiling();
				}
			}
			return hdt;
		}
	}

	@Override
	public HDT doHDTDiff(String hdtFileName1, String hdtFileName2, HDTOptions hdtFormat, ProgressListener listener)
			throws IOException {
		try (HDT hdt1 = loadOrMapHDT(hdtFileName1, listener, hdtFormat);
				HDT hdt2 = loadOrMapHDT(hdtFileName2, listener, hdtFormat)) {
			HDTImpl hdt = new HDTImpl(hdtFormat);
			try (Profiler profiler = Profiler.createOrLoadSubSection("hdtDiff", hdtFormat, true)) {
				hdt.diff(hdt1, hdt2, listener, profiler);
			}
			return hdt;
		}
	}

	@Override
	protected HDT doHDTDiffBit(String location, String hdtFileName, Bitmap deleteBitmap, HDTOptions hdtFormat,
			ProgressListener listener) throws IOException {
		try (HDT hdtOriginal = loadOrMapHDT(hdtFileName, listener, hdtFormat)) {
			HDTImpl hdt = new HDTImpl(hdtFormat);
			try (Profiler profiler = Profiler.createOrLoadSubSection("hdtDiffBit", hdtFormat, true)) {
				hdt.diffBit(location, hdtOriginal, deleteBitmap, listener, profiler);
			} catch (Throwable t) {
				try {
					throw t;
				} finally {
					hdt.close();
				}
			}
			return hdt;
		}
	}

	@Override
	protected HDT doHDTCatTree(RDFFluxStop fluxStop, HDTSupplier supplier, String filename, String baseURI,
			RDFNotation rdfNotation, HDTOptions hdtFormat, ProgressListener listener)
			throws IOException, ParserException {
		try (InputStream is = IOUtil.getFileInputStream(filename)) {
			return doHDTCatTree(fluxStop, supplier, is, baseURI, rdfNotation, hdtFormat, listener);
		}
	}

	@Override
	protected HDT doHDTCatTree(RDFFluxStop fluxStop, HDTSupplier supplier, InputStream stream, String baseURI,
			RDFNotation rdfNotation, HDTOptions hdtFormat, ProgressListener listener)
			throws IOException, ParserException {
		RDFParserCallback parser = RDFParserFactory.getParserCallback(rdfNotation, hdtFormat);
		try (PipedCopyIterator<TripleString> iterator = RDFParserFactory.readAsIterator(parser, stream, baseURI, true,
				rdfNotation)) {
			return doHDTCatTree(fluxStop, supplier, iterator, baseURI, hdtFormat, listener);
		}
	}

	@Override
	protected HDT doHDTCatTree(RDFFluxStop fluxStop, HDTSupplier supplier, Iterator<TripleString> iterator,
			String baseURI, HDTOptions hdtFormat, ProgressListener listener) throws IOException, ParserException {
		try (CatTreeImpl tree = new CatTreeImpl(hdtFormat)) {
			return tree.doGeneration(fluxStop, supplier, iterator, baseURI, listener);
		}
	}

	@Override
	protected HDT doHDTCat(List<String> hdtFileNames, HDTOptions hdtFormat, ProgressListener listener)
			throws IOException {
		if (hdtFileNames.isEmpty()) {
			return HDTFactory.createHDT(hdtFormat);
		}
		if (hdtFileNames.size() == 1) {
			return loadOrMapHDT(hdtFileNames.get(0), listener, hdtFormat);
		}

		try (KCatImpl kCat = new KCatImpl(hdtFileNames, hdtFormat, listener)) {
			return kCat.cat();
		}
	}

	@Override
	protected HDT doHDTDiffBitCat(List<String> hdtFileNames, List<? extends Bitmap> deleteBitmaps, HDTOptions hdtFormat,
			ProgressListener listener) throws IOException {
		if (hdtFileNames.isEmpty()) {
			return HDTFactory.createHDT(hdtFormat);
		}

		if (hdtFileNames.size() != deleteBitmaps.size()) {
			throw new IllegalArgumentException("hdtFileNames.size() != deleteBitmaps.size()");
		}

		try (KCatImpl kCat = new KCatImpl(hdtFileNames, deleteBitmaps, hdtFormat, listener)) {
			return kCat.cat();
		}
	}

}
