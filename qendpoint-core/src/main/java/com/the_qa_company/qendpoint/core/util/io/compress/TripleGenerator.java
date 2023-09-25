package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.triples.TripleID;

import java.util.Iterator;

/**
 * Utility class to generate triples
 */
public class TripleGenerator {
	public static Iterator<TripleID> of(long triples, boolean quads) {
		if (quads) {
			return new Iterator<>() {
				private long current = 1;

				@Override
				public boolean hasNext() {
					return current <= triples;
				}

				@Override
				public TripleID next() {
					long c = current++;
					return new TripleID(c, c, c, c);
				}
			};
		}
		return new Iterator<>() {
			private long current = 1;

			@Override
			public boolean hasNext() {
				return current <= triples;
			}

			@Override
			public TripleID next() {
				long c = current++;
				return new TripleID(c, c, c);
			}
		};
	}
}
