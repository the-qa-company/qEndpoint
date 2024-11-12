package com.the_qa_company.qendpoint.core.iterator.utils;

import com.the_qa_company.qendpoint.core.rdf.RDFFluxStop;
import com.the_qa_company.qendpoint.core.triples.TripleString;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

public class FluxStopTripleStringIteratorImpl {
	private static class Impl implements FluxStopTripleStringIterator {
		private TripleString next;
		private final Iterator<TripleString> iterator;
		private final RDFFluxStop fluxStop;
		private boolean stop;

		public Impl(Iterator<TripleString> iterator, RDFFluxStop fluxStop) {
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

		@Override
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

	private static class ImplCount implements FluxStopTripleStringIterator {
		private TripleString next;
		private final Iterator<TripleString> iterator;
		private final RDFFluxStop fluxStop;
		private boolean stop;
		private long totalCount;

		public ImplCount(Iterator<TripleString> iterator, RDFFluxStop fluxStop) {
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

		@Override
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
				totalCount++;
				return next;
			} finally {
				next = null;
			}
		}

		@Override
		public boolean supportCount() {
			return true;
		}

		@Override
		public long getTotalCount() {
			return totalCount;
		}
	}

	private static class ImplCountAsync implements FluxStopTripleStringIterator {
		private TripleString next;
		private final Iterator<TripleString> iterator;
		private final RDFFluxStop fluxStop;
		private boolean stop;
		private final AtomicLong totalCount = new AtomicLong();

		public ImplCountAsync(Iterator<TripleString> iterator, RDFFluxStop fluxStop) {
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

		@Override
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
				totalCount.incrementAndGet();
				return next;
			} finally {
				next = null;
			}
		}

		@Override
		public boolean supportCount() {
			return true;
		}

		@Override
		public long getTotalCount() {
			return totalCount.get();
		}
	}

	public static FluxStopTripleStringIterator newInstance(Iterator<TripleString> iterator, RDFFluxStop fluxStop,
			boolean supportCount, boolean async) {
		if (supportCount) {
			if (async) {
				return new ImplCountAsync(iterator, fluxStop);
			}
			return new ImplCount(iterator, fluxStop);
		}
		return new Impl(iterator, fluxStop);
	}
}
