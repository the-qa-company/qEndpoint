package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.hdt.HDTManagerImpl;
import com.the_qa_company.qendpoint.core.hdt.HDTSupplier;
import com.the_qa_company.qendpoint.core.iterator.utils.FluxStopTripleStringIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.FluxStopTripleStringIteratorImpl;
import com.the_qa_company.qendpoint.core.iterator.utils.PipedCopyIterator;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.rdf.RDFFluxStop;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.rdf.RDFParserFactory;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.Profiler;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.listener.IntermediateListener;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

public class ChunkGenImpl {
	private final ForkJoinPool pool;
	private final ProgressListener listener;
	private final HDTSupplier supplier;
	private final boolean supportCount;
	private final long memoryCountBest;
	private final AtomicReference<Throwable> exception = new AtomicReference<>();

	public ChunkGenImpl(int numWorkers, HDTOptions spec, ProgressListener listener) {
		this.pool = new ForkJoinPool(numWorkers);
		this.supplier = HDTSupplier.fromSpec(spec);
		this.listener = ProgressListener.ofNullable(listener);
		supportCount = spec.getBoolean(HDTOptionsKeys.LOADER_CATTREE_SUPPORT_COUNT, false);
		long triplesCount = HDTManagerImpl.findBestMemoryChunkDiskMapTreeCat() / numWorkers / 4;

		double factor = spec.getDouble(HDTOptionsKeys.LOADER_CATTREE_MEMORY_FAULT_FACTOR, 1.4);
		if (factor <= 0) {
			throw new IllegalArgumentException(
					HDTOptionsKeys.LOADER_CATTREE_MEMORY_FAULT_FACTOR + " can't have a negative or 0 value!");
		}

		memoryCountBest = Math.max(128, (long) (triplesCount * factor));
	}

	public Future<Path> genFile(Path in, Path out, String baseURI, HDTOptions spec) {
		RDFFluxStop stop = spec.getFluxStop(HDTOptionsKeys.RDF_FLUX_STOP_KEY,
				() -> RDFFluxStop.countLimit(memoryCountBest));

		return pool.submit(() -> {
			if (exception.get() != null)
				return null;
			RDFNotation notation = RDFNotation.guess(in);

			RDFParserCallback parser = RDFParserFactory.getParserCallback(notation, spec);

			try (InputStream is = IOUtil.getFileInputStream(in.toAbsolutePath().toString());
					PipedCopyIterator<TripleString> iterator = RDFParserFactory.readAsIterator(parser, is, baseURI,
							true, notation, spec)) {
				HDTOptions specTmp = spec.pushTop();
				Path workdir = Path.of("workdir").resolve(in.getFileName());

				specTmp.set(HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY, workdir.resolve("future.hdt"));
				specTmp.set(HDTOptionsKeys.LOADER_CATTREE_FUTURE_HDT_LOCATION_KEY, workdir.resolve("cfuture.hdt"));
				specTmp.set(HDTOptionsKeys.HDTCAT_LOCATION, workdir.resolve("hdtcat"));
				specTmp.set(HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, workdir.resolve("gendisk"));
				specTmp.set(HDTOptionsKeys.LOADER_CATTREE_LOCATION_KEY, workdir.resolve("cattree"));

				FluxStopTripleStringIterator it = FluxStopTripleStringIteratorImpl.newInstance(iterator, stop,
						supportCount, true);
				int id = 0;
				while (it.hasNextFlux()) {
					Path outIt = out.resolveSibling(out.getFileName() + "." + id + ".hdt");
					try (Profiler prof = new Profiler("gen " + in.getFileName())) {
						prof.setDisabled(false);
						prof.pushSection("generation");
						specTmp.set(HDTOptionsKeys.PROFILER_KEY, prof);
						IntermediateListener il = new IntermediateListener(listener,
								"file#" + outIt.getFileName() + " - ");

						// force the garbage collection
						System.gc();

						supplier.doGenerateHDT(it, baseURI, specTmp, il, outIt);
						prof.popSection();

						prof.writeToDisk(outIt.resolveSibling(outIt.getFileName() + ".prof"));
					}
					id++;
				}
				return out;
			} catch (Throwable t) {
				exception.accumulateAndGet(t, (or, t2) -> {
					if (or == null)
						return t2;
					or.addSuppressed(t2);
					return or;
				});
				throw t;
			}
		});
	}

	public Throwable getException() {
		return exception.get();
	}

	public void crashAnyIo() throws IOException {
		Throwable ex = getException();
		if (ex == null) {
			return;
		}

		if (ex instanceof IOException ioe) {
			throw ioe;
		}
		if (ex instanceof RuntimeException re) {
			throw re;
		}
		if (ex instanceof Error er) {
			throw er;
		}
		throw new RuntimeException(ex);
	}
}
