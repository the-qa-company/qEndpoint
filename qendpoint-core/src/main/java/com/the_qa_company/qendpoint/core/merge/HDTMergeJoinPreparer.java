package com.the_qa_company.qendpoint.core.merge;

import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.util.CommonUtils;
import com.the_qa_company.qendpoint.core.util.string.ByteString;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class HDTMergeJoinPreparer {
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
				occP[1 - (int)pp]++;
			}

			long ss = patt.getSubject();
			long oo = patt.getObject();

			if (ss < 0) {
				occSH[1 - (int)ss]++;
			}
			if (oo < 0) {
				if (ss != oo) { // avoid double var
					occSH[1 - (int)oo]++;
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
}
