package com.the_qa_company.qendpoint.core.search.utils.iterator;

import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.iterator.utils.FetcherIterator;
import com.the_qa_company.qendpoint.core.search.component.VarPattern;
import com.the_qa_company.qendpoint.core.triples.TripleID;

import java.util.Iterator;

/**
 * basic filter removing triples not matching a {@link VarPattern}, this
 * iterator will have to convert the pid to oid or sid, reducing the speed for
 * CXX, XXC and XXX patterns.
 *
 * @author Antoine Willerval
 */
public class DupeSearchIterator extends FetcherIterator<TripleID> {

	private final HDT hdt;
	private final Iterator<TripleID> iterator;
	private final VarPattern varPattern;

	public DupeSearchIterator(HDT hdt, Iterator<TripleID> iterator, VarPattern varPattern) {
		this.hdt = hdt;
		this.iterator = iterator;
		this.varPattern = varPattern;
	}

	@Override
	protected TripleID getNext() {
		while (iterator.hasNext()) {
			TripleID tid = iterator.next();

			// is this thing match the pattern?
			if (varPattern.test(hdt, tid.getSubject(), tid.getPredicate(), tid.getObject())) {
				return tid;
			}
		}
		return null;
	}
}
