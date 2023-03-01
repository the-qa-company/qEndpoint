package com.the_qa_company.qendpoint.core.hdt.impl.converter;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.iterator.utils.EmptyIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.FetcherIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.PeekIterator;
import com.the_qa_company.qendpoint.core.triples.TripleID;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Simple version of
 * {@link com.the_qa_company.qendpoint.core.dictionary.impl.kcat.GroupBySubjectMapIterator}
 * with only the sort by the object.
 */
public class ObjectReSortIterator extends FetcherIterator<TripleID> {
	private final Comparator<TripleID> tripleIDComparator;
	private final PeekIterator<TripleID> ids;
	private Iterator<TripleID> next = EmptyIterator.of();

	public ObjectReSortIterator(Iterator<TripleID> ids, TripleComponentOrder order) {
		this.ids = new PeekIterator<>(ids);
		switch (order) {
		case SPO, PSO -> tripleIDComparator = Comparator.comparingLong(TripleID::getObject);
		case SOP, POS -> tripleIDComparator = Comparator.comparingLong(TripleID::getPredicate)
				.thenComparingLong(TripleID::getObject);
		default -> // OSP, OPS, Unknown
			// ngl, I'm not even sure it will work for triple order != SPO
			throw new IllegalArgumentException("Can't resort triple component of type: " + order);
		}
	}

	@Override
	protected TripleID getNext() {
		if (next.hasNext()) {
			return next.next();
		}

		if (!ids.hasNext()) {
			return null;
		}

		List<TripleID> next = new ArrayList<>();
		TripleID id = ids.next().clone();
		next.add(id);

		long s = id.getSubject();
		long p = id.getPredicate();

		while (ids.hasNext()) {
			TripleID nextTriple = ids.peek();
			if (!(nextTriple.getSubject() == s && nextTriple.getPredicate() == p)) {
				break;
			}
			next.add(ids.next().clone());
		}
		// we don't need to compare other components because the (s,p) are equal
		next.sort(tripleIDComparator);
		this.next = next.iterator();
		return this.next.next();
	}
}
