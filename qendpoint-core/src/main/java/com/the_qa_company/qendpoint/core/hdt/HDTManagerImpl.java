package com.the_qa_company.qendpoint.core.hdt;

import com.the_qa_company.qendpoint.core.enums.CompressionType;
import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.exceptions.SameChecksumException;
import com.the_qa_company.qendpoint.core.hdt.impl.HDTDiskImporter;
import com.the_qa_company.qendpoint.core.hdt.impl.HDTImpl;
import com.the_qa_company.qendpoint.core.hdt.impl.TempHDTImporterOnePass;
import com.the_qa_company.qendpoint.core.hdt.impl.TempHDTImporterTwoPass;
import com.the_qa_company.qendpoint.core.hdt.writer.TripleWriterHDT;
import com.the_qa_company.qendpoint.core.header.HeaderUtil;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsFile;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.options.HDTSpecification;
import com.the_qa_company.qendpoint.core.rdf.RDFFluxStop;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.rdf.RDFParserFactory;
import com.the_qa_company.qendpoint.core.rdf.TripleWriter;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.Profiler;
import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.dictionary.impl.MultipleSectionDictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.kcat.KCatImpl;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.CatTreeImpl;
import com.the_qa_company.qendpoint.core.iterator.utils.MapIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.PipedCopyIterator;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
	public HDTResult doLoadHDT(String hdtFileName, ProgressListener listener, HDTOptions spec) throws IOException {
		HDTPrivate hdt = new HDTImpl(spec);
		hdt.loadFromHDT(hdtFileName, listener);
		return HDTResult.of(hdt);
	}

	@Override
	protected HDTResult doMapHDT(String hdtFileName, ProgressListener listener, HDTOptions spec) throws IOException {
		HDTPrivate hdt = new HDTImpl(spec);
		hdt.mapFromHDT(new File(hdtFileName), 0, listener);
		return HDTResult.of(hdt);
	}

	@Override
	public HDTResult doLoadHDT(InputStream hdtFile, ProgressListener listener, HDTOptions spec) throws IOException {
		HDTPrivate hdt = new HDTImpl(spec);
		hdt.loadFromHDT(hdtFile, listener);
		return HDTResult.of(hdt);
	}

	@Override
	public HDTResult doLoadIndexedHDT(String hdtFileName, ProgressListener listener, HDTOptions spec)
			throws IOException {
		HDTPrivate hdt = new HDTImpl(spec);
		hdt.loadFromHDT(hdtFileName, listener);
		hdt.loadOrCreateIndex(listener, spec);
		return HDTResult.of(hdt);
	}

	@Override
	public HDTResult doMapIndexedHDT(String hdtFileName, ProgressListener listener, HDTOptions spec)
			throws IOException {
		HDTPrivate hdt = new HDTImpl(spec);
		hdt.mapFromHDT(new File(hdtFileName), 0, listener);
		hdt.loadOrCreateIndex(listener, spec);
		return HDTResult.of(hdt);
	}

	@Override
	public HDTResult doLoadIndexedHDT(InputStream hdtFile, ProgressListener listener, HDTOptions spec)
			throws IOException {
		HDTPrivate hdt = new HDTImpl(spec);
		hdt.loadFromHDT(hdtFile, listener);
		hdt.loadOrCreateIndex(listener, spec);
		return HDTResult.of(hdt);
	}

	@Override
	public HDTResult doIndexedHDT(HDT hdt, ProgressListener listener, HDTOptions spec) throws IOException {
		((HDTPrivate) hdt).loadOrCreateIndex(listener, HDTOptions.ofNullable(spec));
		return HDTResult.of(hdt);
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

	private HDTResult generateChecksumAfter(long checksum, Path checksumPath, HDTOptions spec, HDTResult res)
			throws IOException {
		try {
			if (checksumPath != null) {
				// save the current checksum in a file
				Files.writeString(checksumPath, String.valueOf(checksum));
			}
		} catch (Throwable t) {
			try {
				res.close();
			} catch (Throwable t2) {
				t.addSuppressed(t2);
			}
			throw t;
		}
		return res;
	}

	@Override
	public HDTResult doGenerateHDT(String rdfFileName, String baseURI, RDFNotation rdfNotation, HDTOptions spec,
			ProgressListener listener) throws IOException, ParserException {
		// choose the importer
		String loaderType = spec.get(HDTOptionsKeys.LOADER_TYPE_KEY);
		TempHDTImporter loader;
		boolean isQuad = rdfNotation == RDFNotation.NQUAD;
		if (isQuad) {
			if (!spec.contains(HDTOptionsKeys.TEMP_DICTIONARY_IMPL_KEY)) {
				spec.set(HDTOptionsKeys.TEMP_DICTIONARY_IMPL_KEY, HDTOptionsKeys.TEMP_DICTIONARY_IMPL_VALUE_HASH_QUAD);
			}
			if (!spec.contains(HDTOptionsKeys.DICTIONARY_TYPE_KEY)) {
				spec.set(HDTOptionsKeys.DICTIONARY_TYPE_KEY, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_QUAD_SECTION);
			}
		}

		Path preDownload = null;
		Path checksumPath = null;
		long checksum = 0;
		try {
			if (spec.getBoolean(HDTOptionsKeys.LOADER_PREDOWNLOAD_URL) && IOUtil.isRemoteURL(rdfFileName)) {
				spec = spec.pushTop();
				spec.set(HDTOptionsKeys.LOADER_PREDOWNLOAD_URL, false); // disable
																		// for
																		// cattree
				long retry = spec.getInt(HDTOptionsKeys.LOADER_PREDOWNLOAD_URL_RETRY, 1);

				final String rdfFileName2 = rdfFileName;
				preDownload = spec.getPath(HDTOptionsKeys.LOADER_PREDOWNLOAD_URL_FILE, () -> {
					try {
						return Files.createTempFile("hdtPreDlUrl", IOUtil.getSuffix(rdfFileName2));
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				});
				Path dlFile = preDownload.resolveSibling(preDownload.getFileName() + ".download");
				HDTOptionsFile opFile = new HDTOptionsFile(dlFile);

				long tryCount = 1;
				checksumPath = spec.getPath(HDTOptionsKeys.LOADER_PREDOWNLOAD_CHECKSUM_PATH);
				while (true) {
					opFile.sync();
					long trueSize = IOUtil.getContentLengthLong(rdfFileName);
					long lastLength = opFile.getOptions().getInt("last-length", 0);
					long preSize;

					if (lastLength != trueSize) {
						preSize = 0;
					} else {
						try {
							preSize = Files.size(preDownload);
						} catch (IOException ignore) {
							preSize = 0;
						}
					}

					StandardOpenOption[] openOptions;
					if (preSize <= 0) {
						openOptions = new StandardOpenOption[] { StandardOpenOption.CREATE,
								StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE };
					} else {
						openOptions = new StandardOpenOption[] { StandardOpenOption.CREATE, StandardOpenOption.APPEND,
								StandardOpenOption.WRITE };
					}
					opFile.getOptions().set("last-length", trueSize);
					opFile.save();

					listener.notifyProgress(0, "predownload " + rdfFileName + " into " + preDownload + " try #"
							+ tryCount + " / preSize " + preSize);

					IOUtil.HTTPData readIs = IOUtil.getFileInputStreamData(rdfFileName, false, preSize);
					long trueCurrSize = readIs.conn().getContentLengthLong();
					if (trueCurrSize > 0 && trueSize != trueCurrSize) {
						logger.error("Find bad file sizes compared to the previous calls, {} != {}", trueCurrSize,
								trueSize);
						continue; // try again, can it create an infinite loop
									// is the server is bad?
					}
					InputStream stream = readIs.is();

					try (InputStream is = checksumPath != null ? new CRCInputStream(stream, new CRC32()) : stream;
							OutputStream os = new FastBufferedOutputStream(
									Files.newOutputStream(preDownload, openOptions))) {
						IOUtil.copy(is, os, listener, 10_000_000);
						if (is instanceof CRCInputStream crcIs) {
							checksum = crcIs.getCRC().getValue();
						}
						break;
					} catch (IOException e) {
						if (tryCount == retry) {
							throw new IOException("Can't predownload " + rdfFileName + " into " + preDownload
									+ " after " + tryCount + " tries", e);
						}
						tryCount++;
						logger.error("Exception when predownloading {}", rdfFileName, e);
					}
				}
				rdfFileName = preDownload.toAbsolutePath().toString();

				if (checksumPath != null) {
					if (spec.getBoolean(HDTOptionsKeys.LOADER_PREDOWNLOAD_CHECKSUM_FAIL_SAME, false)) {
						// check the previous checksum compared with the old one
						try {
							if (Long.parseLong(Files.readString(checksumPath)) == checksum) {
								throw new SameChecksumException();
							}
						} catch (NoSuchFileException | NumberFormatException ignore) {
						}
					}
				}
			}

			if (HDTOptionsKeys.LOADER_TYPE_VALUE_DISK.equals(loaderType)) {
				return generateChecksumAfter(checksum, checksumPath, spec, doGenerateHDTDisk(rdfFileName, baseURI,
						rdfNotation, CompressionType.guess(rdfFileName), spec, listener));
			} else if (HDTOptionsKeys.LOADER_TYPE_VALUE_CAT.equals(loaderType)) {
				return generateChecksumAfter(checksum, checksumPath, spec, doHDTCatTree(readFluxStopOrSizeLimit(spec),
						HDTSupplier.fromSpec(spec), rdfFileName, baseURI, rdfNotation, spec, listener));
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

				return generateChecksumAfter(checksum, checksumPath, spec, HDTResult.of(hdt));
			}
		} finally {
			try {
				if (preDownload != null && !spec.getBoolean(HDTOptionsKeys.LOADER_PREDOWNLOAD_URL_NO_DELETE, false)) {
					Files.deleteIfExists(preDownload);
				}
			} catch (IOException e) {
				logger.error("Can't delete predownload temp file", e);
			}
		}
	}

	@Override
	public HDTResult doGenerateHDT(InputStream fileStream, String baseURI, RDFNotation rdfNotation,
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
	public HDTResult doGenerateHDT(Iterator<TripleString> triples, String baseURI, HDTOptions spec,
			ProgressListener listener) throws IOException {
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
			try {
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
				return HDTResult.of(hdt);
			} catch (Throwable t) {
				try {
					hdt.close();
				} catch (Throwable t2) {
					t.addSuppressed(t2);
				}
				throw t;
			}
		}
	}

	@Override
	public HDTResult doGenerateHDTDisk(String rdfFileName, String baseURI, RDFNotation rdfNotation,
			CompressionType compressionType, HDTOptions hdtFormat, ProgressListener listener)
			throws IOException, ParserException {
		if (compressionType == CompressionType.NONE) {
			RDFParserCallback parser = RDFParserFactory.getParserCallback(rdfNotation, hdtFormat);
			try (PipedCopyIterator<TripleString> iterator = RDFParserFactory.readAsIterator(parser, rdfFileName,
					baseURI, true, rdfNotation)) {
				return doGenerateHDTDisk0(iterator, true, baseURI, hdtFormat, listener);
			}
		}
		try (InputStream stream = IOUtil.getFileInputStream(rdfFileName, false)) {
			return doGenerateHDTDisk(stream, baseURI, rdfNotation, compressionType, hdtFormat, listener);
		}
	}

	@Override
	public HDTResult doGenerateHDTDisk(InputStream fileStream, String baseURI, RDFNotation rdfNotation,
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
	public HDTResult doGenerateHDTDisk(Iterator<TripleString> iterator, String baseURI, HDTOptions hdtFormat,
			ProgressListener progressListener) throws IOException, ParserException {
		return doGenerateHDTDisk0(iterator, hdtFormat.getBoolean(HDTOptionsKeys.LOADER_DISK_NO_COPY_ITERATOR_KEY),
				baseURI, hdtFormat, progressListener);
	}

	private HDTResult doGenerateHDTDisk0(Iterator<TripleString> iterator, boolean copyIterator, String baseURI,
			HDTOptions hdtFormat, ProgressListener progressListener) throws IOException, ParserException {
		try (HDTDiskImporter hdtDiskImporter = new HDTDiskImporter(hdtFormat, progressListener, baseURI)) {
			if (copyIterator) {
				return HDTResult.of(hdtDiskImporter.runAllSteps(iterator));
			} else {
				// create a copy of the triple at loading time to avoid weird
				// behaviors
				return HDTResult
						.of(hdtDiskImporter.runAllSteps(new MapIterator<>(iterator, TripleString::tripleToString)));
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
	public HDTResult doHDTCat(String location, String hdtFileName1, String hdtFileName2, HDTOptions hdtFormat,
			ProgressListener listener) throws IOException {
		if (!hdtFormat.getBoolean(HDTOptionsKeys.HDTCAT_LEGACY, false)) {
			HDTOptions hdtOptions = hdtFormat.pushTop();
			hdtOptions.set(HDTOptionsKeys.HDTCAT_LOCATION, location);
			return doHDTCat(List.of(hdtFileName1, hdtFileName2), hdtOptions, listener);
		}
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
			return HDTResult.of(hdt);
		}
	}

	@Override
	public HDTResult doHDTDiff(String hdtFileName1, String hdtFileName2, HDTOptions hdtFormat,
			ProgressListener listener) throws IOException {
		try (HDT hdt1 = loadOrMapHDT(hdtFileName1, listener, hdtFormat);
				HDT hdt2 = loadOrMapHDT(hdtFileName2, listener, hdtFormat)) {
			HDTImpl hdt = new HDTImpl(hdtFormat);
			try (Profiler profiler = Profiler.createOrLoadSubSection("hdtDiff", hdtFormat, true)) {
				hdt.diff(hdt1, hdt2, listener, profiler);
			}
			return HDTResult.of(hdt);
		}
	}

	@Override
	protected HDTResult doHDTDiffBit(String location, String hdtFileName, Bitmap deleteBitmap, HDTOptions hdtFormat,
			ProgressListener listener) throws IOException {
		if (!hdtFormat.getBoolean(HDTOptionsKeys.HDTCAT_LEGACY, false)) {
			HDTOptions hdtOptions = hdtFormat.pushTop();
			hdtOptions.set(HDTOptionsKeys.HDTCAT_LOCATION, location);
			return doHDTDiffBitCat(List.of(hdtFileName), List.of(deleteBitmap), hdtOptions, listener);
		}
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
			return HDTResult.of(hdt);
		}
	}

	@Override
	protected HDTResult doHDTCatTree(RDFFluxStop fluxStop, HDTSupplier supplier, String filename, String baseURI,
			RDFNotation rdfNotation, HDTOptions hdtFormat, ProgressListener listener)
			throws IOException, ParserException {
		RDFParserCallback parser = RDFParserFactory.getParserCallback(rdfNotation, hdtFormat);
		try (PipedCopyIterator<TripleString> iterator = RDFParserFactory.readAsIterator(parser, filename, baseURI, true,
				rdfNotation)) {
			return doHDTCatTree(fluxStop, supplier, iterator, baseURI, hdtFormat, listener);
		}
	}

	@Override
	protected HDTResult doHDTCatTree(RDFFluxStop fluxStop, HDTSupplier supplier, InputStream stream, String baseURI,
			RDFNotation rdfNotation, HDTOptions hdtFormat, ProgressListener listener)
			throws IOException, ParserException {
		RDFParserCallback parser = RDFParserFactory.getParserCallback(rdfNotation, hdtFormat);
		try (PipedCopyIterator<TripleString> iterator = RDFParserFactory.readAsIterator(parser, stream, baseURI, true,
				rdfNotation)) {
			return doHDTCatTree(fluxStop, supplier, iterator, baseURI, hdtFormat, listener);
		}
	}

	@Override
	protected HDTResult doHDTCatTree(RDFFluxStop fluxStop, HDTSupplier supplier, Iterator<TripleString> iterator,
			String baseURI, HDTOptions hdtFormat, ProgressListener listener) throws IOException, ParserException {
		try (CatTreeImpl tree = new CatTreeImpl(hdtFormat)) {
			return tree.doGeneration(fluxStop, supplier, iterator, baseURI, listener);
		}
	}

	@Override
	protected HDTResult doHDTCat(List<String> hdtFileNames, HDTOptions hdtFormat, ProgressListener listener)
			throws IOException {
		if (hdtFileNames.isEmpty()) {
			return HDTResult.of(HDTFactory.createHDT(hdtFormat));
		}
		if (hdtFileNames.size() == 1) {
			return HDTResult.of(loadOrMapHDT(hdtFileNames.get(0), listener, hdtFormat));
		}

		try (KCatImpl kCat = KCatImpl.of(hdtFileNames, hdtFormat, listener)) {
			return HDTResult.of(kCat.cat());
		}
	}

	@Override
	protected HDTResult doHDTDiffBitCat(List<String> hdtFileNames, List<? extends Bitmap> deleteBitmaps,
			HDTOptions hdtFormat, ProgressListener listener) throws IOException {
		if (hdtFileNames.isEmpty()) {
			return HDTResult.of(HDTFactory.createHDT(hdtFormat));
		}

		if (hdtFileNames.size() != deleteBitmaps.size()) {
			throw new IllegalArgumentException("hdtFileNames.size() != deleteBitmaps.size()");
		}

		try (KCatImpl kCat = KCatImpl.of(hdtFileNames, deleteBitmaps, hdtFormat, listener)) {
			return HDTResult.of(kCat.cat());
		}
	}

	@Override
	protected HDTResult doHDTDiffBitCatObject(List<HDT> hdtFileNames, List<? extends Bitmap> deleteBitmaps,
			HDTOptions hdtFormat, ProgressListener listener, boolean closeHDTs) throws IOException {
		if (hdtFileNames.isEmpty()) {
			return HDTResult.of(HDTFactory.createHDT(hdtFormat));
		}

		if (hdtFileNames.size() != deleteBitmaps.size()) {
			throw new IllegalArgumentException("hdtFileNames.size() != deleteBitmaps.size()");
		}

		try (KCatImpl kCat = KCatImpl.of(hdtFileNames, deleteBitmaps, hdtFormat, listener, closeHDTs)) {
			return HDTResult.of(kCat.cat());
		}
	}

}
