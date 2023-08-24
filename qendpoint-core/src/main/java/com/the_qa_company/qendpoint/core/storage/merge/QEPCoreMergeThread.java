package com.the_qa_company.qendpoint.core.storage.merge;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.dictionary.impl.kcat.KCatMapping;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.ControlInfo;
import com.the_qa_company.qendpoint.core.options.ControlInformation;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.storage.QEPCore;
import com.the_qa_company.qendpoint.core.storage.QEPCoreContext;
import com.the_qa_company.qendpoint.core.storage.QEPCoreException;
import com.the_qa_company.qendpoint.core.storage.QEPCoreOptions;
import com.the_qa_company.qendpoint.core.storage.QEPDataset;
import com.the_qa_company.qendpoint.core.storage.QEPDatasetContext;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class QEPCoreMergeThread extends Thread {
	private static final String STEP_HDC = "hdc";
	private static final String STEP_MAP_BUILDING = "map_building";
	private static final String OPT_DSIDS = "ds";
	private static final String OPT_DSIDS_SEPARATOR = ":";
	private final QEPCore core;
	private boolean runMerge;
	private boolean completed;
	private final AtomicReference<Throwable> exception = new AtomicReference<>();
	private final Object callMergeEventObj = new Object() {};
	private final Object endMergeEventObj = new Object() {};

	private final int clusterSize;
	private final double epsilon;
	private final MergeFindFunc mergeFindFunc;
	private final boolean progressMerge;

	public QEPCoreMergeThread(QEPCore core, HDTOptions options) {
		super("QEPCoreMerge #" + core.getLocation().toString().hashCode());
		this.core = core;

		clusterSize = options.getInt32(QEPCoreOptions.QEPC_MERGE_THRESHOLD, 10);
		mergeFindFunc = MergeFindFunc.readOption(options.get(QEPCoreOptions.QEPC_MERGE_NUMFUNC));
		epsilon = options.getDouble(QEPCoreOptions.QEPC_MERGE_EPSILON, 4);
		progressMerge = options.getBoolean(QEPCoreOptions.QEPC_MERGE_PROGRESS, true);

		if (clusterSize < 2) {
			throw new IllegalArgumentException(
					format("Can't have a %s smaller than 2", QEPCoreOptions.QEPC_MERGE_THRESHOLD));
		}
		if (epsilon <= 0) {
			throw new IllegalArgumentException(
					format("Can't have a %s smaller than or equals to 0", QEPCoreOptions.QEPC_MERGE_EPSILON));
		}
	}

	/**
	 * @return the file where the merge information are loaded
	 */
	public Path getMergeFile() {
		return core.getLocation().resolve("merge.bin");
	}

	/**
	 * @return the directory where the merge is running
	 */
	public Path getMergeLocation() {
		return core.getLocation().resolve("merge-work");
	}

	public ControlInfo loadMergeFile() throws IOException {
		ControlInfo ci = new ControlInformation();
		try {
			ci.load(getMergeFile());
		} catch (FileNotFoundException e) {
			return null;
		}
		if (ci.getType() != ControlInfo.Type.QEPCORE_MERGE) {
			throw new IOException("Not a QEPCore Merge file");
		}
		return ci;
	}

	public void saveMergeFile(String type, HDTOptions options) throws IOException {
		ControlInformation ci = new ControlInformation();
		ci.setFormat(type);
		ci.setType(ControlInfo.Type.QEPCORE_MERGE);
		for (Object key : options.getKeys()) {
			String k = String.valueOf(key);
			ci.set(k, options.get(k));
		}
		ci.save(getMergeFile());
	}

	private void checkException() {
		Throwable throwable = exception.getAndSet(null);
		if (throwable != null) {
			try {
				throw throwable;
			} catch (Error | RuntimeException er) {
				throw er;
			} catch (Throwable t) {
				throw new QEPCoreException(t);
			}
		}
	}

	/**
	 * ask a merge to the thread
	 */
	public void askMerge() {
		checkException();
		synchronized (callMergeEventObj) {
			runMerge = true;
			callMergeEventObj.notifyAll();
		}
	}

	/**
	 * wait for the merge thread to end
	 *
	 * @return if the merge thread has been completed
	 * @throws InterruptedException interruption while waiting for the end
	 */
	public boolean waitMerge() throws InterruptedException {
		synchronized (endMergeEventObj) {
			if (completed) {
				return true;
			}
			endMergeEventObj.wait();
			return completed;
		}
	}

	private record WeightContext(QEPDatasetContext ctx, long value) {}

	public Collection<WeightContext> findCluster(Collection<QEPDatasetContext> dss) {
		List<WeightContext> ctx = dss.stream().map(c -> new WeightContext(c, mergeFindFunc.mapValue(c)))
				.sorted(Comparator.comparingLong(WeightContext::value)).toList();

		if (ctx.size() <= 1) {
			return ctx; // trivial case
		}

		List<WeightContext> currentCluster = new ArrayList<>();
		List<List<WeightContext>> clusters = new ArrayList<>();
		WeightContext lastDs = ctx.get(0);
		currentCluster.add(lastDs);

		for (int i = 1; i < ctx.size(); i++) {
			WeightContext ds = ctx.get(i);

			if (ds.value <= lastDs.value + epsilon) {
				currentCluster.add(ds);
			} else {
				clusters.add(currentCluster);
				currentCluster = new ArrayList<>();
				currentCluster.add(ds);
			}
			lastDs = ds;
		}
		clusters.add(currentCluster);

		return clusters.stream().max(Comparator.comparingLong(List::size)).orElseThrow();
	}

	@Override
	public void run() {
		mainLoop:
		while (true) {
			synchronized (callMergeEventObj) {
				while (!runMerge) {
					try {
						callMergeEventObj.wait();
					} catch (InterruptedException e) {
						break mainLoop;
					}
					if (isInterrupted()) {
						break mainLoop;
					}
				}
				runMerge = false;
			}
			ProgressListener listener = progressMerge ? core.getListener() : ProgressListener.ignore();
			// we can start the merge process
			try (QEPCoreContext context = core.createSearchContext()) {
				listener.notifyProgress(0, "searching merge cluster...");
				Collection<QEPDatasetContext> dss = context.getContexts();

				// find cluster
				Collection<WeightContext> cluster = findCluster(dss);

				if (cluster.size() < clusterSize) {
					listener.notifyProgress(100, "merge cluster too small " + cluster.size() + "/" + clusterSize);
					continue; // the cluster isn't big enough
				}

				// write cluster file
				saveMergeFile(STEP_HDC, HDTOptions.of(OPT_DSIDS, cluster.stream().map(w -> w.ctx.dataset().id())
						.collect(Collectors.joining(OPT_DSIDS_SEPARATOR))));

				List<HDT> ds = new ArrayList<>();
				List<Bitmap> dsDelete = new ArrayList<>();
				for (WeightContext wc : cluster) {
					QEPDatasetContext ctx = wc.ctx;
					ds.add(ctx.dataset().dataset());
					dsDelete.add(ctx.deleteBitmap());
				}

				Path mergeLocation = getMergeLocation();
				Path mergeMapsDir = mergeLocation.resolve("kcmaps");
				Path workDir = mergeLocation.resolve("diffcat");
				Path output = workDir.resolve("output.hdt");
				Files.createDirectories(workDir);

				// HCD

				HDTOptions options = core.getOptions().pushTop();
				options.setOptions(
						// store maps
						HDTOptionsKeys.HDTCAT_STORE_MAPS, mergeMapsDir,
						// set the right future location
						HDTOptionsKeys.HDTCAT_FUTURE_LOCATION, output,
						// set the work dir
						HDTOptionsKeys.HDTCAT_LOCATION, workDir);

				listener.notifyProgress(10, "starting datasets merge process");
				try (HDT diffCat = HDTManager.diffBitCatHDTObject(ds, dsDelete, options,
						listener.sub(10, 70, "diffcat: "), false)) {
					diffCat.saveToHDT(output, listener.sub(70, 75, "saving diffcat: "));

					// start linking
					String newDatasetId = core.createNewDatasetId();
					QEPDataset nds = new QEPDataset(core, newDatasetId, null, diffCat, null, null);

					// pause updates

					// write merge location file

				}
				try (KCatMapping kcmapping = new KCatMapping(mergeMapsDir)) {
					// copy merge file to dataset
					// remove pointer to the cluster dataset

					// remove merge file location

					// allow updates

				}

				// delete temp merge file
				Files.deleteIfExists(getMergeFile());
				runMerge = false;
			} catch (Throwable e) {
				exception.accumulateAndGet(e, (a, b) -> {
					if (b == null) {
						return a;
					}
					if (a == null) {
						return b;
					}
					if (a instanceof Error) {
						a.addSuppressed(b);
						return b;
					}
					if (b instanceof Error) {
						b.addSuppressed(a);
						return b;
					}
					if (a instanceof RuntimeException) {
						a.addSuppressed(b);
						return b;
					}
					if (b instanceof RuntimeException) {
						b.addSuppressed(a);
						return b;
					}
					a.addSuppressed(b);
					return a;
				});
			}
		}
		synchronized (endMergeEventObj) {
			completed = true;
			endMergeEventObj.notifyAll();
		}
	}
}
