package com.the_qa_company.qendpoint.core.hdt.impl;

import com.the_qa_company.qendpoint.core.dictionary.DictionaryFactory;
import com.the_qa_company.qendpoint.core.dictionary.DictionaryPrivate;
import com.the_qa_company.qendpoint.core.dictionary.impl.CompressFourSectionDictionary;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.CompressTripleMapper;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.CompressionResult;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.MapOnCallHDT;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.TripleCompressionResult;
import com.the_qa_company.qendpoint.core.header.HeaderPrivate;
import com.the_qa_company.qendpoint.core.iterator.utils.AsyncIteratorFetcher;
import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.TempTriples;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.triples.TriplesPrivate;
import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.Profiler;
import com.the_qa_company.qendpoint.core.util.StringUtil;
import com.the_qa_company.qendpoint.core.util.concurrent.KWayMerger;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.compress.MapCompressTripleMerger;
import com.the_qa_company.qendpoint.core.util.io.compress.TripleGenerator;
import com.the_qa_company.qendpoint.core.util.listener.IntermediateListener;
import com.the_qa_company.qendpoint.core.util.listener.ListenerUtil;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

/**
 * HDT Disk generation methods
 *
 * @author Antoine Willerval
 */
public class HDTDiskImporter implements Closeable {
	/**
	 * @return ram on the system
	 */
	public static long getAvailableMemory() {
		Runtime runtime = Runtime.getRuntime();
		return (runtime.maxMemory() - (runtime.totalMemory() - runtime.freeMemory()));
	}

	/**
	 * @return a theoretical maximum amount of memory the JVM will attempt to
	 *         use
	 */
	static long getMaxChunkSize(int workers) {
		return (long) (getAvailableMemory() * 0.85 / (1.5 * 3 * workers));
	}

	// configs
	private final HDTOptions hdtFormat;
	private final MultiThreadListener listener;
	private final String compressMode;
	private final String futureHDTLocation;
	private final Path futureHDTLocationPath;
	private final CloseSuppressPath basePath;
	private final long chunkSize;
	private final int ways;
	private final int workers;
	private final int bufferSize;
	private final boolean mapHDT;
	private final boolean debugHDTBuilding;
	private final Profiler profiler;
	private final HDTBase<? extends HeaderPrivate, ? extends DictionaryPrivate, ? extends TriplesPrivate> hdt;
	private long rawSize;

	// component status
	private boolean dict = false;
	private boolean header = false;
	private boolean triples = false;

	public HDTDiskImporter(HDTOptions hdtFormat, ProgressListener progressListener, String baseURI) throws IOException {
		this.hdtFormat = hdtFormat;
		listener = ListenerUtil.multiThreadListener(progressListener);
		// load config

		// compression mode
		compressMode = hdtFormat.get(HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_KEY,
				HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_VALUE_COMPLETE); // see
																				// CompressionResult
																				// worker
																				// for
																				// compression
																				// tasks
		workers = (int) hdtFormat.getInt(HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY,
				Runtime.getRuntime()::availableProcessors);
		// check and set default values if required
		if (workers <= 0) {
			throw new IllegalArgumentException("Number of workers should be positive!");
		}
		// maximum size of a chunk
		chunkSize = hdtFormat.getInt(HDTOptionsKeys.LOADER_DISK_CHUNK_SIZE_KEY, () -> getMaxChunkSize(this.workers));
		if (chunkSize < 0) {
			throw new IllegalArgumentException("Negative chunk size!");
		}
		long maxFileOpenedLong = hdtFormat.getInt(HDTOptionsKeys.LOADER_DISK_MAX_FILE_OPEN_KEY, 1024);
		int maxFileOpened;
		if (maxFileOpenedLong < 0 || maxFileOpenedLong > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("maxFileOpened should be positive!");
		} else {
			maxFileOpened = (int) maxFileOpenedLong;
		}
		long kwayLong = hdtFormat.getInt(HDTOptionsKeys.LOADER_DISK_KWAY_KEY,
				() -> Math.max(1, BitUtil.log2(maxFileOpened / this.workers)));
		if (kwayLong <= 0 || kwayLong > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("kway can't be negative!");
		} else {
			ways = (int) kwayLong;
		}

		long bufferSizeLong = hdtFormat.getInt(HDTOptionsKeys.LOADER_DISK_BUFFER_SIZE_KEY,
				CloseSuppressPath.BUFFER_SIZE);
		if (bufferSizeLong > Integer.MAX_VALUE - 5L || bufferSizeLong <= 0) {
			throw new IllegalArgumentException("Buffer size can't be negative or bigger than the size of an array!");
		} else {
			bufferSize = (int) bufferSizeLong;
		}
		// location of the working directory, will be deleted after generation
		String baseNameOpt = hdtFormat.get(HDTOptionsKeys.LOADER_DISK_LOCATION_KEY);
		// location of the future HDT file, do not set to create the HDT in
		// memory while mergin
		futureHDTLocation = hdtFormat.get(HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY);
		futureHDTLocationPath = futureHDTLocation == null ? null : Path.of(futureHDTLocation);

		profiler = Profiler.createOrLoadSubSection("doGenerateHDTDisk", hdtFormat, true);
		try {
			if (baseNameOpt == null || baseNameOpt.isEmpty()) {
				basePath = CloseSuppressPath.of(Files.createTempDirectory("hdt-java-generate-disk"));
			} else {
				basePath = CloseSuppressPath.of(baseNameOpt);
			}
			basePath.closeWithDeleteRecurse();
			mapHDT = futureHDTLocation != null && !futureHDTLocation.isEmpty();
			// debug the build strategy
			debugHDTBuilding = hdtFormat.getBoolean("debug.disk.build");

			// create working directory
			basePath.mkdirs();

			if (!mapHDT) {
				// using default implementation
				hdt = new HDTImpl(hdtFormat);
			} else {
				// using map implementation
				hdt = new WriteHDTImpl(hdtFormat, basePath.resolve("maphdt"), bufferSize);
			}
			hdt.setBaseUri(baseURI);
		} catch (Throwable t) {
			try {
				throw t;
			} finally {
				profiler.close();
			}
		}
	}

	/**
	 * create the Dictonary of the HDT
	 *
	 * @param iterator the triples stream to create the dictionary
	 * @throws java.lang.IllegalArgumentException is the component was already
	 *                                            built
	 */
	public CompressTripleMapper compressDictionary(Iterator<TripleString> iterator)
			throws ParserException, IOException {
		if (this.dict) {
			throw new IllegalArgumentException("Dictionary already built! Use another importer instance!");
		}
		listener.notifyProgress(0,
				"Sorting sections with chunk of size: " + StringUtil.humanReadableByteCount(chunkSize, true) + "B with "
						+ ways + "ways and " + workers + " worker(s)");

		AsyncIteratorFetcher<TripleString> source = new AsyncIteratorFetcher<>(iterator);

		profiler.pushSection("section compression");
		CompressionResult compressionResult;
		try {
			compressionResult = DictionaryFactory
					.createSectionCompressor(hdtFormat, basePath.resolve("sectionCompression"), source, listener,
							bufferSize, chunkSize, 1 << ways, hdtFormat.getBoolean("debug.disk.slow.stream2"))
					.compress(workers, compressMode);
		} catch (KWayMerger.KWayMergerException | InterruptedException e) {
			throw new ParserException(e);
		}
		profiler.popSection();

		listener.unregisterAllThreads();
		listener.notifyProgress(20, "Create sections and triple mapping");

		profiler.pushSection("dictionary write");
		// create sections and triple mapping
		DictionaryPrivate dictionary = hdt.getDictionary();
		CompressTripleMapper mapper = new CompressTripleMapper(basePath, compressionResult.getTripleCount(), chunkSize,
				compressionResult.supportsGraph(),
				compressionResult.supportsGraph() ? compressionResult.getGraphCount() : 0);
		try (CompressFourSectionDictionary modifiableDictionary = new CompressFourSectionDictionary(compressionResult,
				mapper, listener, debugHDTBuilding, compressionResult.supportsGraph())) {
			dictionary.loadAsync(modifiableDictionary, listener);
		} catch (InterruptedException e) {
			throw new ParserException(e);
		}
		profiler.popSection();

		// complete the mapper with the shared count and delete compression data
		compressionResult.delete();
		rawSize = compressionResult.getRawSize();
		mapper.setShared(dictionary.getNshared());

		this.dict = true;
		return mapper;
	}

	/**
	 * create the Triples of the HDT
	 *
	 * @param mapper the mapper from the dictionary building
	 * @throws java.lang.IllegalArgumentException is the component was already
	 *                                            built
	 */
	public void compressTriples(CompressTripleMapper mapper) throws ParserException, IOException {
		if (this.triples) {
			throw new IllegalArgumentException("Triples already built! Use another importer instance!");
		}
		listener.notifyProgress(40, "Create mapped and sort triple file");
		// create mapped triples file
		TripleCompressionResult tripleCompressionResult;
		TriplesPrivate triples = hdt.getTriples();
		TripleComponentOrder order = triples.getOrder();
		profiler.pushSection("triple compression/map");
		try {
			MapCompressTripleMerger tripleMapper = new MapCompressTripleMerger(basePath.resolve("tripleMapper"),
					new AsyncIteratorFetcher<>(TripleGenerator.of(mapper.getTripleCount(), mapper.supportsGraph())),
					mapper, listener, order, bufferSize, chunkSize, 1 << ways,
					mapper.supportsGraph() ? mapper.getGraphsCount() : 0);
			tripleCompressionResult = tripleMapper.merge(workers, compressMode);
		} catch (KWayMerger.KWayMergerException | InterruptedException e) {
			throw new ParserException(e);
		}
		profiler.popSection();
		listener.unregisterAllThreads();

		profiler.pushSection("bit triple creation");
		try {
			// create bit triples and load the triples
			TempTriples tempTriples = tripleCompressionResult.getTriples();
			IntermediateListener il = new IntermediateListener(listener);
			il.setRange(80, 90);
			il.setPrefix("Create bit triples: ");
			il.notifyProgress(0, "create triples");
			triples.load(tempTriples, il);
			tempTriples.close();

			// completed the triples, delete the mapper
			mapper.delete();
		} finally {
			tripleCompressionResult.close();
		}
		profiler.popSection();

		this.triples = true;
	}

	/**
	 * create the Header of the HDT
	 *
	 * @throws java.lang.IllegalArgumentException is the component was already
	 *                                            built
	 */
	public void createHeader() {
		if (this.header) {
			throw new IllegalArgumentException("Header already built! Use another importer instance!");
		}
		profiler.pushSection("header creation");

		listener.notifyProgress(90, "Create HDT header");
		// header
		hdt.populateHeaderStructure(hdt.getBaseURI());
		hdt.getHeader().insert("_:statistics", HDTVocabulary.ORIGINAL_SIZE, rawSize);

		profiler.popSection();

		this.header = true;
	}

	/**
	 * create the HDT from the parts of the previous methods
	 *
	 * @return hdt
	 * @throws IOException                        io exception during disk
	 *                                            generation
	 * @throws java.lang.IllegalArgumentException is a component is missing
	 */
	public HDT convertToHDT() throws IOException {
		if (!this.dict) {
			throw new IllegalArgumentException("Dictionary missing, can't create HDT");
		}
		if (!this.triples) {
			throw new IllegalArgumentException("Triples missing, can't create HDT");
		}
		if (!this.header) {
			throw new IllegalArgumentException("Header missing, can't create HDT");
		}
		// return the HDT
		if (mapHDT) {
			profiler.pushSection("map to hdt");
			// write the HDT and map it
			try {
				hdt.saveToHDT(futureHDTLocation, listener);
			} finally {
				hdt.close();
			}
			IntermediateListener il = new IntermediateListener(listener);
			il.setPrefix("Map HDT: ");
			il.setRange(95, 100);
			il.notifyProgress(0, "start");
			try {
				return new MapOnCallHDT(futureHDTLocationPath);
			} finally {
				profiler.popSection();
			}
		} else {
			listener.notifyProgress(100, "HDT completed");
			return hdt;
		}
	}

	/**
	 * call all the step to create an HDT
	 *
	 * @param iterator the iterator to load the data
	 * @return hdt
	 * @throws IOException                        io exception during disk
	 *                                            generation
	 * @throws ParserException                    parsing exception during disk
	 *                                            generation
	 * @throws java.lang.IllegalArgumentException is a component is missing
	 */
	public HDT runAllSteps(Iterator<TripleString> iterator) throws IOException, ParserException {
		// compress the triples into sections and compressed triples
		CompressTripleMapper mapper = compressDictionary(iterator);
		compressTriples(mapper);
		createHeader();

		return convertToHDT();
	}

	@Override
	public void close() throws IOException {
		try {
			profiler.stop();
			profiler.writeProfiling();
			listener.notifyProgress(100, "Clearing disk");
		} finally {
			try {
				basePath.close();
			} finally {
				profiler.close();
			}
		}
	}
}
