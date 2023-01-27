package com.the_qa_company.qendpoint.core.iterator.utils;

import com.the_qa_company.qendpoint.core.rdf.RDFFluxStop;
import com.the_qa_company.qendpoint.core.triples.TripleString;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class FluxStopTripleStringIterator implements Iterator<TripleString> {
	private TripleString next;
	private final Iterator<TripleString> iterator;
	private final RDFFluxStop fluxStop;
	private boolean stop;

	public FluxStopTripleStringIterator(Iterator<TripleString> iterator, RDFFluxStop fluxStop) {
		this.iterator = iterator;
		this.fluxStop = fluxStop;
	}

	@Override
	public boolean hasNext() {
		if (stop) {
			return false;
		}
		if (next != null) {
			return true;
		}

		if (!iterator.hasNext()) {
			return false;
		}

		next = iterator.next();

		if (!fluxStop.canHandle(next)) {
			stop = true;
			return false;
		}

		return true;
	}

	/**
	 * @return if a new flux can be extracted, will restart the flux
	 */
	public boolean hasNextFlux() {
		stop = false;
		fluxStop.restart();
		return hasNext();
	}

	@Override
	public TripleString next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		try {
			return next;
		} finally {
			next = null;
		}
	}

}
