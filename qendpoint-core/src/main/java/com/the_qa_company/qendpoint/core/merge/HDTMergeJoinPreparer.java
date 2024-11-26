package com.the_qa_company.qendpoint.core.merge;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.quad.QuadString;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.CommonUtils;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class HDTMergeJoinPreparer {
	private static final Logger logger = LoggerFactory.getLogger(HDTMergeJoinPreparer.class);
	private final HDT hdt;
	private final List<TripleID> patterns = new ArrayList<>();
	private int keyIds;

	public HDTMergeJoinPreparer(HDT hdt) {
		this.hdt = hdt;
	}

	public long createVar() {
		return -(++keyIds);
	}

	public void addPattern(TripleID tid) {
		patterns.add(tid);
	}

	public void addPattern(long s, long p, long o) {
		addPattern(new TripleID(s, p, o));
	}

	public List<HDTMergeJoinIterator> buildIteration() {
		List<HDTMergeJoinIterator> lst = new ArrayList<>();

		if (keyIds == 0) {
			// no var
			throw new NotImplementedException("No variable");// TODO:
		}
		int[] occSH = new int[keyIds];
		int[] occP = new int[keyIds];

		for (TripleID patt : patterns) {
			long pp = patt.getPredicate();
			if (pp < 0) {
				occP[1 - (int) pp]++;
			}

			long ss = patt.getSubject();
			long oo = patt.getObject();

			if (ss < 0) {
				occSH[1 - (int) ss]++;
			}
			if (oo < 0) {
				if (ss != oo) { // avoid double var
					occSH[1 - (int) oo]++;
				}
			}
		}

		int maxShIdx = CommonUtils.maxArg(occSH);
		int maxPrIdx = CommonUtils.maxArg(occP);

		if (maxShIdx == 0 && maxPrIdx == 0) {
			// no var
			throw new NotImplementedException("No variable");// TODO:
		}

		// fixme: we should also check if all the sub graphs are connected

		if (maxShIdx > maxPrIdx) {
			// load shared var
		} else {
			// load

		}

		return lst;
	}

	public static void main(String[] args) throws IOException, ParserException {
		if (args.length < 2) {
			logger.error("Missing param: Usage [hdt] [desc]");
			return;
		}
		logger.info("Test preparer");
		String hdtPath = args[0];

		record TPData(TripleString ts, TripleComponentRole role, TripleComponentOrder order) {}

		List<TPData> data = new ArrayList<>();

		try (BufferedReader r = Files.newBufferedReader(Path.of(args[1]))) {
			String line;

			while ((line = r.readLine()) != null) {
				if (line.isEmpty() || line.charAt(0) == '#')
					continue; // comment

				QuadString ts = new QuadString();
				ts.read(line, true);

				if (ts.getSubject().equals("var"))
					ts.setSubject(null);
				if (ts.getPredicate().equals("var"))
					ts.setPredicate(null);
				if (ts.getObject().equals("var"))
					ts.setObject(null);

				logger.info("read {}", ts);

				String orderCfg = ts.getGraph().toString();

				if (orderCfg.isEmpty()) {
					logger.error("Invalid role cfg: empty");
					return;
				}
				String[] cfg = orderCfg.split(":");
				TripleComponentRole role = TripleComponentRole.valueOf(cfg[0]);
				TripleComponentOrder order = TripleComponentOrder.valueOf(cfg[1]);
				ts.setGraph(null);

				data.add(new TPData(new TripleString(ts), role, order));
			}
		}

		logger.info("Config loaded");
		data.forEach(c -> logger.info("- {}", c));
		logger.info("Loading HDT for query");
		HDTOptions spec = HDTOptions.of("bitmaptriples.index.allowOldOthers", true);
		try (HDT hdt = HDTManager.mapIndexedHDT(hdtPath, spec, ProgressListener.sout())) {
			List<HDTMergeJoinIterator.MergeIteratorData> mergeData = new ArrayList<>();
			for (TPData tpd : data) {
				TripleID tid = hdt.getDictionary().toTripleId(tpd.ts());

				if (tid.isNoMatch()) {
					logger.error("Triple {} is invalid", tpd.ts());
					return;
				}

				mergeData.add(new HDTMergeJoinIterator.MergeIteratorData(hdt.getTriples().search(tid, tpd.order().mask),
						tpd.role()));
			}

			HDTMergeJoinIterator it = new HDTMergeJoinIterator(mergeData);

			logger.info("results:");
			StopWatch sw = new StopWatch();
			sw.reset();
			long ret = 0;
			while (it.hasNext()) {
				it.next();
				ret++;
				// logger.info("- {}", it.next());
			}
			logger.info("Done in {} {}", sw.stopAndShow(), ret);
		}
		logger.info("Unmapped");
	}

}
