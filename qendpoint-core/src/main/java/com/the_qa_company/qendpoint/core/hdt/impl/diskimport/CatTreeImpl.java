package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
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
	 * @param maxFiles max file to merge
	 * @return list of HDT to merge with current, mi
	 */
	private List<HDTFile> getNextHDTs(boolean nextFile, List<HDTFile> files, HDTFile current, int maxFiles) {
		if (files.isEmpty()) {
			return List.of();
		}
		List<HDTFile> next = new ArrayList<>();
		if (nextFile || files.size() > maxFiles) {
			for (int i = 1; i < maxFiles && i <= files.size(); i++) {
				HDTFile old = files.get(files.size() - i);

				// check if the chunks are matching
				if (nextFile && old.getChunks() > current.getChunks()) {
					break;
				}

				next.add(old);
			}
			if (!nextFile || next.size() == maxFiles - 1) {
				// we have all the elements, or we have enough file
				// we remove the elements from the files
				for (int i = 0; i < next.size(); i++) {
					files.remove(files.size() - 1);
				}
			} else {
				return List.of();
			}
		} else {
			next.addAll(files);
			files.clear();
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
	public HDT doGeneration(RDFFluxStop fluxStop, HDTSupplier supplier, Iterator<TripleString> iterator, String baseURI,
			ProgressListener listener) throws IOException, ParserException {
		if (async && kHDTCat > 1) {
			return doGenerationAsync(fluxStop, supplier, iterator, baseURI, listener);
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
	public HDT doGenerationSync(RDFFluxStop fluxStop, HDTSupplier supplier, Iterator<TripleString> iterator,
			String baseURI, ProgressListener listener) throws IOException, ParserException {
		FluxStopTripleStringIterator it = new FluxStopTripleStringIterator(iterator, fluxStop);

		List<HDTFile> files = new ArrayList<>();

		long gen = 0;
		long cat = 0;

		Path hdtStore = basePath.resolve("hdt-store");
		Path hdtCatLocationPath = basePath.resolve("cat");
		String hdtCatLocation = hdtCatLocationPath.toAbsolutePath().toString();

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
				while (!files.isEmpty()
						&& (!nextFile || (files.get(files.size() - 1)).getChunks() <= hdtFile.getChunks())) {
					HDTFile lastHDTFile = files.remove(files.size() - 1);
					cat++;
					profiler.pushSection("catHDT #" + cat);
					PrefixListener ilc = PrefixListener.of("cat#" + cat, listener);
					Path hdtCatFileLocation = hdtStore.resolve("hdtcat-" + cat + ".hdt");
					try (HDT abcat = HDTManager.catHDT(hdtCatLocation,
							lastHDTFile.getHdtFile().toAbsolutePath().toString(),
							hdtFile.getHdtFile().toAbsolutePath().toString(), hdtFormat, ilc)) {
						abcat.saveToHDT(hdtCatFileLocation.toAbsolutePath().toString(), ilc);
					}
					ilc.clearThreads();
					// delete previous chunks
					Files.delete(lastHDTFile.getHdtFile());
					Files.delete(hdtFile.getHdtFile());
					// note the new hdt file and the number of chunks
					hdtFile = new HDTFile(hdtCatFileLocation, lastHDTFile.getChunks() + hdtFile.getChunks());

					profiler.popSection();
				}
			} else { // kcat
				List<HDTFile> nextHDTs;

				while (!(nextHDTs = getNextHDTs(nextFile, files, hdtFile, kHDTCat)).isEmpty()) {
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
							.map(f -> f.getHdtFile().toAbsolutePath().toString()).collect(Collectors.toList()),
							hdtFormat, ilc)) {
						abcat.saveToHDT(hdtCatFileLocation.toAbsolutePath().toString(), ilc);
					}

					hdtFormat.overrideValue(HDTOptionsKeys.LOADER_CATTREE_FUTURE_HDT_LOCATION_KEY, null);

					ilc.clearThreads();

					// delete previous chunks
					for (HDTFile nextHDT : nextHDTs) {
						Files.delete(nextHDT.getHdtFile());
					}
					// note the new hdt file and the number of chunks
					long chunks = nextHDTs.stream().mapToLong(HDTFile::getChunks).sum();
					hdtFile = new HDTFile(hdtCatFileLocation, chunks);

					profiler.popSection();
				}
			}
			assert nextFile || files.isEmpty() : "no data remaining, but contains files";
			files.add(hdtFile);
		} while (nextFile);

		listener.notifyProgress(100, "done, loading HDT");

		Path hdtFile = files.get(0).hdtFile;

		assert files.size() == 1 : "more than 1 file: " + files;
		assert cat < gen : "more cat than gen";
		assert files.get(0).getChunks() == gen
				: "gen size isn't the same as excepted: " + files.get(0).getChunks() + " != " + gen;

		try {
			// if a future HDT location has been asked, move to it and map the
			// HDT
			if (futureHDTLocation != null) {
				Files.createDirectories(futureHDTLocation.toAbsolutePath().getParent());
				Files.deleteIfExists(futureHDTLocation);
				Files.move(hdtFile, futureHDTLocation);
				return new MapOnCallHDT(futureHDTLocation);
			}

			// if no future location has been asked, load the HDT and delete it
			// after
			return HDTManager.loadHDT(hdtFile.toAbsolutePath().toString());
		} finally {
			Files.deleteIfExists(hdtFile);
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

	static class HDTFile {
		private final Path hdtFile;
		private final long chunks;

		public HDTFile(Path hdtFile, long chunks) {
			this.hdtFile = hdtFile;
			this.chunks = chunks;
		}

		public long getChunks() {
			return chunks;
		}

		public Path getHdtFile() {
			return hdtFile;
		}

		@Override
		public String toString() {
			return "HDTFile{" + "hdtFile=" + hdtFile + ", chunks=" + chunks + '}';
		}
	}
}
