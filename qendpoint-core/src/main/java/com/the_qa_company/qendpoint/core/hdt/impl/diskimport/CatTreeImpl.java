package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.hdt.HDTResult;
import com.the_qa_company.qendpoint.core.hdt.HDTSupplier;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.options.HideHDTOptions;
import com.the_qa_company.qendpoint.core.rdf.RDFFluxStop;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.Profiler;
import com.the_qa_company.qendpoint.core.iterator.utils.FluxStopTripleStringIterator;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import com.the_qa_company.qendpoint.core.util.listener.PrefixListener;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * code for managing the Cat-Tree disk generation
 *
 * @author Antoine Willerval
 */
public class CatTreeImpl implements Closeable {
	private final HideHDTOptions hdtFormat;
	private final int kHDTCat;
	private final int maxHDTCat;
	private final Path basePath;
	private final Path futureHDTLocation;
	private final Closer closer = Closer.of();
	private final Profiler profiler;
	private final boolean async;

	/**
	 * create implementation
	 *
	 * @param hdtFormat hdt format
	 * @throws IOException io exception
	 */
	public CatTreeImpl(HDTOptions hdtFormat) throws IOException {
		try {

			long khdtCatOpt = hdtFormat.getInt(HDTOptionsKeys.LOADER_CATTREE_KCAT, 1);

			if (khdtCatOpt > 0 && khdtCatOpt < Integer.MAX_VALUE - 6) {
				kHDTCat = (int) khdtCatOpt;
			} else {
				throw new IllegalArgumentException("Invalid kcat value: " + khdtCatOpt);
			}

			long maxHDTCatOpt = hdtFormat.getInt(HDTOptionsKeys.LOADER_CATTREE_MAX_FILES, 1);

			if (maxHDTCatOpt > 0 && maxHDTCatOpt < Integer.MAX_VALUE - 6) {
				maxHDTCat = (int) maxHDTCatOpt;
			} else {
				throw new IllegalArgumentException("Invalid max kcat value: " + maxHDTCatOpt);
			}

			String baseNameOpt = hdtFormat.get(HDTOptionsKeys.LOADER_CATTREE_LOCATION_KEY);

			if (baseNameOpt == null || baseNameOpt.isEmpty()) {
				basePath = Files.createTempDirectory("hdt-java-cat-tree");
			} else {
				basePath = Path.of(baseNameOpt);
			}

			futureHDTLocation = Optional
					.ofNullable(hdtFormat.get(HDTOptionsKeys.LOADER_CATTREE_FUTURE_HDT_LOCATION_KEY)).map(Path::of)
					.orElse(null);

			boolean async = hdtFormat.getBoolean(HDTOptionsKeys.LOADER_CATTREE_ASYNC_KEY, false);
			// hide the loader type to avoid infinite recursion
			this.hdtFormat = new HideHDTOptions(hdtFormat, this::mapHiddenKeys);

			if (async) {
				long worker = hdtFormat.getInt(HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY, -1);

				int processors;
				if (worker == -1) {
					processors = Runtime.getRuntime().availableProcessors();
				} else if (worker >= 0 && worker < Integer.MAX_VALUE) {
					processors = (int) worker;
				} else {
					throw new IllegalArgumentException("Bad worker count: " + worker);
				}
				if (processors >= 2) {
					// use one thread to merge the HDTs
					this.hdtFormat.overrideValue(HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY, processors - 1);
					this.async = true;
				} else {
					// not enough processor to run async
					this.async = false;
				}
			} else {
				this.async = false;
			}

			profiler = Profiler.createOrLoadSubSection("doHDTCatTree", hdtFormat, true);
			closer.with((Closeable) profiler::close);

		} catch (Throwable t) {
			try {
				throw t;
			} finally {
				close();
			}
		}
	}

	private String mapHiddenKeys(String key) {
		if (HDTOptionsKeys.LOADER_TYPE_KEY.equals(key)) {
			return HDTOptionsKeys.LOADER_CATTREE_LOADERTYPE_KEY;
		}
		return key;
	}

	/**
	 * get the previous HDTs to merge with current
	 *
	 * @param nextFile if we can create a new HDT after this one
	 * @param files    hdt files to merge
	 * @param current  current created HDT
	 * @return list of HDT to merge with current
	 */
	private List<HDTFile> getNextHDTs(boolean nextFile, List<HDTFile> files, HDTFile current) {
		assert maxHDTCat <= kHDTCat;
		if (files.isEmpty()) {
			return List.of();
		}
		List<HDTFile> next = new ArrayList<>();

		if (nextFile) {
			// we are still indexing after than

			for (int i = 1; i < kHDTCat && i <= files.size(); i++) {
				HDTFile old = files.get(files.size() - i);

				// check if the chunks are matching
				if (old.chunks() > current.chunks()) {
					break;
				}

				next.add(old);
			}
			if (next.size() != kHDTCat - 1) {
				return List.of(); // not enough file, we can wait until we have
									// what to merge
			}

			// we have all the elements, or we have enough file
			// we remove the elements from the files
			for (int i = 0; i < next.size(); i++) {
				files.remove(files.size() - 1);
			}

			next.add(current);
			return next;
		}

		// no next file
		if (files.size() > kHDTCat) {
			// we merge kHDTCat - 1 element with the current one
			for (int i = 1; i < kHDTCat; i++) {
				next.add(files.remove(files.size() - 1));
			}

			next.add(current);
			return next;
		}

		// we are under the maximum amount, we need to merge to reach the
		// minimum

		if (files.size() < maxHDTCat) {
			return List.of(); // acceptable amount of files
		}

		int count = files.size() - (maxHDTCat - 1);

		for (int i = 0; i < count; i++) {
			next.add(files.remove(files.size() - 1));
		}

		next.add(current);
		return next;
	}

	/**
	 * generate the HDT from the stream
	 *
	 * @param fluxStop flux stop
	 * @param supplier hdt supplier
	 * @param iterator triple string stream
	 * @param baseURI  base URI
	 * @param listener progression listener
	 * @return hdt
	 * @throws IOException     io exception
	 * @throws ParserException parsing exception returned by the hdt supplier
	 */
	public HDTResult doGeneration(RDFFluxStop fluxStop, HDTSupplier supplier, Iterator<TripleString> iterator,
			String baseURI, ProgressListener listener) throws IOException, ParserException {
		if (async && kHDTCat > 1 && maxHDTCat == 1) {
			return HDTResult.of(doGenerationAsync(fluxStop, supplier, iterator, baseURI, listener));
		} else {
			return doGenerationSync(fluxStop, supplier, iterator, baseURI, listener);
		}
	}

	/**
	 * generate the HDT from the stream using ASYNC algorithm
	 *
	 * @param fluxStop flux stop
	 * @param supplier hdt supplier
	 * @param iterator triple string stream
	 * @param baseURI  base URI
	 * @param listener progression listener
	 * @return hdt
	 * @throws IOException     io exception
	 * @throws ParserException parsing exception returned by the hdt supplier
	 */
	public HDT doGenerationAsync(RDFFluxStop fluxStop, HDTSupplier supplier, Iterator<TripleString> iterator,
			String baseURI, ProgressListener listener) throws IOException, ParserException {
		try (AsyncCatTreeWorker worker = new AsyncCatTreeWorker(this, fluxStop, supplier, iterator, baseURI,
				listener)) {
			worker.start();
			return worker.buildHDT();
		}
	}

	/**
	 * generate the HDT from the stream using SYNC algorithm
	 *
	 * @param fluxStop flux stop
	 * @param supplier hdt supplier
	 * @param iterator triple string stream
	 * @param baseURI  base URI
	 * @param listener progression listener
	 * @return hdt
	 * @throws IOException     io exception
	 * @throws ParserException parsing exception returned by the hdt supplier
	 */
	public HDTResult doGenerationSync(RDFFluxStop fluxStop, HDTSupplier supplier, Iterator<TripleString> iterator,
			String baseURI, ProgressListener listener) throws IOException, ParserException {
		FluxStopTripleStringIterator it = new FluxStopTripleStringIterator(iterator, fluxStop);

		List<HDTFile> files = new ArrayList<>();

		long gen = 0;
		long cat = 0;

		Path hdtStore = basePath.resolve("hdt-store");
		Path hdtCatLocationPath = basePath.resolve("cat");

		Files.createDirectories(hdtStore);
		Files.createDirectories(hdtCatLocationPath);

		boolean nextFile;
		do {
			// generate the hdt
			gen++;
			profiler.pushSection("generateHDT #" + gen);
			PrefixListener il = PrefixListener.of("gen#" + gen, listener);
			Path hdtLocation = hdtStore.resolve("hdt-" + gen + ".hdt");
			// help memory flooding algorithm
			System.gc();
			supplier.doGenerateHDT(it, baseURI, hdtFormat, il, hdtLocation);
			il.clearThreads();

			nextFile = it.hasNextFlux();
			HDTFile hdtFile = new HDTFile(hdtLocation, 1);
			profiler.popSection();

			// merge the generated hdt with each block with enough size
			if (kHDTCat == 1) { // default impl
				while (!files.isEmpty() && (!nextFile || (files.get(files.size() - 1)).chunks() <= hdtFile.chunks())) {
					HDTFile lastHDTFile = files.remove(files.size() - 1);
					cat++;
					profiler.pushSection("catHDT #" + cat);
					PrefixListener ilc = PrefixListener.of("cat#" + cat, listener);
					Path hdtCatFileLocation = hdtStore.resolve("hdtcat-" + cat + ".hdt");
					try (HDT abcat = HDTManager.catHDT(hdtCatLocationPath, lastHDTFile.hdtFile(), hdtFile.hdtFile(),
							hdtFormat, ilc)) {
						abcat.saveToHDT(hdtCatFileLocation, ilc);
					}
					ilc.clearThreads();
					// delete previous chunks
					Files.delete(lastHDTFile.hdtFile());
					Files.delete(hdtFile.hdtFile());
					// note the new hdt file and the number of chunks
					hdtFile = new HDTFile(hdtCatFileLocation, lastHDTFile.chunks() + hdtFile.chunks());

					profiler.popSection();
				}
			} else { // kcat
				List<HDTFile> nextHDTs;

				while (!(nextHDTs = getNextHDTs(nextFile, files, hdtFile)).isEmpty()) {
					// merge all the files
					cat++;
					profiler.pushSection("catHDT #" + cat);
					PrefixListener ilc = PrefixListener.of("cat#" + cat, listener);
					Path hdtCatFileLocation = hdtStore.resolve("hdtcat-" + cat + ".hdt");

					assert nextHDTs.size() > 1;

					// override the value to create the cat into
					// hdtCatFileLocation
					hdtFormat.overrideValue(HDTOptionsKeys.LOADER_CATTREE_FUTURE_HDT_LOCATION_KEY,
							hdtCatFileLocation.toAbsolutePath());

					try (HDT abcat = HDTManager.catHDT(nextHDTs.stream()
							.map(f -> f.hdtFile().toAbsolutePath().toString()).collect(Collectors.toList()), hdtFormat,
							ilc)) {
						abcat.saveToHDT(hdtCatFileLocation.toAbsolutePath().toString(), ilc);
					}

					hdtFormat.overrideValue(HDTOptionsKeys.LOADER_CATTREE_FUTURE_HDT_LOCATION_KEY, null);

					ilc.clearThreads();

					// delete previous chunks
					for (HDTFile nextHDT : nextHDTs) {
						Files.delete(nextHDT.hdtFile());
					}
					// note the new hdt file and the number of chunks
					long chunks = nextHDTs.stream().mapToLong(HDTFile::chunks).sum();
					hdtFile = new HDTFile(hdtCatFileLocation, chunks);

					profiler.popSection();
				}
			}
			assert nextFile || files.size() < maxHDTCat
					: "no data remaining, but contains files " + nextFile + " " + files.size() + " " + maxHDTCat;
			files.add(hdtFile);
		} while (nextFile);

		listener.notifyProgress(100, "done, loading HDT");

		assert files.size() > 0 && files.size() <= maxHDTCat : "more than " + maxHDTCat + " file: " + files;

		assert cat < gen : "more cat than gen";
		assert files.stream().mapToLong(HDTFile::chunks).sum() == gen
				: "gen size isn't the same as excepted: " + files.get(files.size() - 1).chunks() + " != " + gen;

		try {
			if (files.size() == 1) {
				Path hdtFile = files.get(0).hdtFile;
				// if a future HDT location has been asked, move to it and map
				// the
				// HDT
				if (futureHDTLocation != null) {
					Files.createDirectories(futureHDTLocation.toAbsolutePath().getParent());
					Files.move(hdtFile, futureHDTLocation, StandardCopyOption.REPLACE_EXISTING);
					return HDTResult.of(new MapOnCallHDT(futureHDTLocation));
				}

				// if no future location has been asked, load the HDT and delete
				// it
				// after
				return HDTResult.of(HDTManager.loadHDT(hdtFile.toAbsolutePath().toString()));
			} else {
				List<HDT> results = new ArrayList<>();
				try {
					if (futureHDTLocation != null) {
						// split the results into files
						Files.createDirectories(futureHDTLocation.toAbsolutePath().getParent());

						int idx = 0;
						for (HDTFile file : files) {
							Path target = futureHDTLocation.resolveSibling(futureHDTLocation.getFileName() + "." + idx);
							Files.move(file.hdtFile, target, StandardCopyOption.REPLACE_EXISTING);
							results.add(new MapOnCallHDT(target));
							idx++;
						}
					} else {
						// load all the hdts
						for (HDTFile file : files) {
							results.add(HDTManager.loadHDT(file.hdtFile.toAbsolutePath().toString()));
						}
					}
				} catch (Throwable t) {
					try {
						Closer.closeSingle(results);
					} catch (Throwable t2) {
						t.addSuppressed(t2);
					}
					throw t;
				}

				return HDTResult.of(results);
			}
		} finally {
			for (HDTFile hdtFile : files) {
				Files.deleteIfExists(hdtFile.hdtFile);
			}
			profiler.stop();
			profiler.writeProfiling();
		}
	}

	public HideHDTOptions getHdtFormat() {
		return hdtFormat;
	}

	public Path getFutureHDTLocation() {
		return futureHDTLocation;
	}

	public Profiler getProfiler() {
		return profiler;
	}

	public int getkHDTCat() {
		return kHDTCat;
	}

	public Path getBasePath() {
		return basePath;
	}

	@Override
	public void close() throws IOException {
		closer.close();
	}

	record HDTFile(Path hdtFile, long chunks) {

		@Override
		public String toString() {
			return "HDTFile{" + "hdtFile=" + hdtFile + ", chunks=" + chunks + '}';
		}
	}
}
