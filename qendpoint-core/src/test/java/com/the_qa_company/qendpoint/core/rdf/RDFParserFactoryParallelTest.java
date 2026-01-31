package com.the_qa_company.qendpoint.core.rdf;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.iterator.utils.PipedCopyIterator;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

public class RDFParserFactoryParallelTest {
	@Test
	public void readAsIteratorHonorsParallelFlagForStreams() throws Exception {
		TrackingParser parser = new TrackingParser();
		HDTOptions spec = HDTOptions.of();
		spec.set(HDTOptionsKeys.PARSER_RIOT_PARALLEL_KEY, "false");

		try (PipedCopyIterator<TripleString> it = RDFParserFactory.readAsIterator(parser,
				new ByteArrayInputStream(new byte[0]), "http://example.org/", false, RDFNotation.NTRIPLES, spec)) {
			while (it.hasNext()) {
				it.next();
			}
		}

		assertEquals(Boolean.FALSE, parser.parallel.get());
	}

	@Test
	public void readAsIteratorForcesSequentialWhenParallelEnabled() throws Exception {
		TrackingParser parser = new TrackingParser();
		HDTOptions spec = HDTOptions.of();
		spec.set(HDTOptionsKeys.PARSER_RIOT_PARALLEL_KEY, "true");

		try (PipedCopyIterator<TripleString> it = RDFParserFactory.readAsIterator(parser,
				new ByteArrayInputStream(new byte[0]), "http://example.org/", false, RDFNotation.NTRIPLES, spec)) {
			while (it.hasNext()) {
				it.next();
			}
		}

		assertEquals(Boolean.FALSE, parser.parallel.get());
	}

	private static final class TrackingParser implements RDFParserCallback {
		private final AtomicReference<Boolean> parallel = new AtomicReference<>();

		@Override
		public void doParse(String fileName, String baseUri, RDFNotation notation, boolean keepBNode,
				RDFCallback callback) {
			parallel.set(null);
		}

		@Override
		public void doParse(InputStream input, String baseUri, RDFNotation notation, boolean keepBNode,
				RDFCallback callback) {
			parallel.set(null);
		}

		@Override
		public void doParse(InputStream input, String baseUri, RDFNotation notation, boolean keepBNode,
				RDFCallback callback, boolean parallel) {
			this.parallel.set(parallel);
		}
	}
}
