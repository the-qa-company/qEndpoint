/**
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/rdf/parsers/RDFParserRIOT.java
 * $ Revision: $Rev: 191 $ Last modified: $Date: 2013-03-03 11:41:43 +0000 (dom,
 * 03 mar 2013) $ Last modified by: $Author: mario.arias $ This library is free
 * software; you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation;
 * version 3.0 of the License. This library is distributed in the hope that it
 * will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 * of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
 * General Public License for more details. You should have received a copy of
 * the GNU Lesser General Public License along with this library; if not, write
 * to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston,
 * MA 02110-1301 USA Contacting the authors: Mario Arias: mario.arias@deri.org
 * Javier D. Fernandez: jfergar@infor.uva.es Miguel A. Martinez-Prieto:
 * migumar2@infor.uva.es
 */

package com.the_qa_company.qendpoint.core.rdf.parsers;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.the_qa_company.qendpoint.core.quad.QuadString;
import org.apache.jena.graph.Triple;
import org.apache.jena.iri.impl.LexerFixer;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.RDFParserBuilder;
import org.apache.jena.riot.lang.LabelToNode;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mario.arias
 */
public class RDFParserRIOT implements RDFParserCallback {
	private static final Logger log = LoggerFactory.getLogger(RDFParserRIOT.class);
	private static final int CORES = Runtime.getRuntime().availableProcessors();
	private static volatile boolean fixedLexer = false;

	private void parse(InputStream stream, String baseUri, Lang lang, boolean keepBNode, RDFCallback callback,
			boolean parallel, boolean strict) {
		int workerStreams = Math.max(1, CORES - 1);

		if (!fixedLexer) {
			synchronized (RDFParserRIOT.class) {
				if (!fixedLexer) {
					LexerFixer.fixLexers();
					fixedLexer = true;
				}
			}
		}

		ElemStringBuffer buffer = new ElemStringBuffer(callback);

		if (parallel && (lang == Lang.TURTLE)) {
			if (keepBNode) {
				ChunkedConcurrentInputStream cs = new ChunkedConcurrentInputStream(stream, workerStreams);
				InputStream bnodes = cs.getBnodeStream();
				InputStream[] streams = cs.getStreams();
				runParallelParsers(bnodes, streams, baseUri, lang, callback, strict);
			} else {
				configureParser(stream, baseUri, lang, false, strict).parse(buffer);
			}
			return;
		}

		if (parallel && (lang == Lang.NQUADS || lang == Lang.NTRIPLES)) {
			if (keepBNode) {
				ConcurrentInputStream cs = new ConcurrentInputStream(stream, workerStreams);
				InputStream bnodes = cs.getBnodeStream();
				InputStream[] streams = cs.getStreams();
				runParallelParsers(bnodes, streams, baseUri, lang, callback, strict);
			} else {
				configureParser(stream, baseUri, lang, false, strict).parse(buffer);
			}
			return;
		}

		configureParser(stream, baseUri, lang, keepBNode, strict).parse(buffer);
	}

	private void runParallelParsers(InputStream bnodeStream, InputStream[] streams, String baseUri, Lang lang,
			RDFCallback callback, boolean strict) {
		List<InputStream> allStreams = new ArrayList<>();
		List<Thread> threads = new ArrayList<>();
		AtomicReference<Throwable> failure = new AtomicReference<>();

		allStreams.add(bnodeStream);
		threads.add(
				buildParserThread(bnodeStream, "BNode parser", baseUri, lang, callback, failure, allStreams, strict));

		for (int i = 0; i < streams.length; i++) {
			InputStream stream = streams[i];
			allStreams.add(stream);
			threads.add(buildParserThread(stream, "Stream parser " + (i + 1), baseUri, lang, callback, failure,
					allStreams, strict));
		}

		threads.forEach(Thread::start);
		for (Thread thread : threads) {
			try {
				while (thread.isAlive()) {
					thread.join(1000);
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
		}

		Throwable thrown = failure.get();
		if (thrown != null) {
			throw new RuntimeException("Parallel parse failed", thrown);
		}
	}

	private Thread buildParserThread(InputStream stream, String name, String baseUri, Lang lang, RDFCallback callback,
			AtomicReference<Throwable> failure, List<InputStream> allStreams, boolean strict) {
		Thread thread = new Thread(() -> {
			try {
				ElemStringBuffer buffer = new ElemStringBuffer(callback);
				configureParser(stream, baseUri, lang, true, strict).parse(buffer);
			} catch (Throwable t) {
				if (failure.compareAndSet(null, t)) {
					closeStreams(allStreams);
				}
			}
		});
		thread.setName(name);
		return thread;
	}

	private static RDFParserBuilder configureParser(InputStream stream, String baseUri, Lang lang, boolean keepBNode,
			boolean strict) {
		RDFParserBuilder builder = RDFParser.source(stream).base(baseUri).lang(lang).strict(strict);
		if (keepBNode) {
			builder.labelToNode(LabelToNode.createUseLabelAsGiven());
		}
		return builder;
	}

	private void closeStreams(List<InputStream> streams) {
		for (InputStream stream : streams) {
			try {
				stream.close();
			} catch (IOException ignored) {
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.rdf.RDFParserCallback#doParse(java.lang.String,
	 * java.lang.String, hdt.enums.RDFNotation,
	 * hdt.rdf.RDFParserCallback.Callback)
	 */
	@Override
	public void doParse(String fileName, String baseUri, RDFNotation notation, boolean keepBNode, RDFCallback callback)
			throws ParserException {
		doParse(fileName, baseUri, notation, keepBNode, callback, true);
	}

	@Override
	public void doParse(String fileName, String baseUri, RDFNotation notation, boolean keepBNode, RDFCallback callback,
			boolean parallel) throws ParserException {
		try (InputStream input = IOUtil.getFileInputStream(fileName)) {
			doParse(input, baseUri, notation, keepBNode, callback, parallel, false);
		} catch (FileNotFoundException e) {
			throw new ParserException(e);
		} catch (Exception e) {
			log.error("Unexpected exception parsing file: {}", fileName, e);
			throw new ParserException(e);
		}
	}

	@Override
	public void doParse(InputStream input, String baseUri, RDFNotation notation, boolean keepBNode,
			RDFCallback callback) throws ParserException {
		doParse(input, baseUri, notation, keepBNode, callback, false, false);
	}

	@Override
	public void doParse(InputStream input, String baseUri, RDFNotation notation, boolean keepBNode,
			RDFCallback callback, boolean parallel) throws ParserException {
		doParse(input, baseUri, notation, keepBNode, callback, parallel, false);
	}

	public void doParse(InputStream input, String baseUri, RDFNotation notation, boolean keepBNode,
			RDFCallback callback, boolean parallel, boolean strict) throws ParserException {
		try {
			switch (notation) {
			case NTRIPLES -> parse(input, baseUri, Lang.NTRIPLES, keepBNode, callback, parallel, strict);
			case NQUAD -> parse(input, baseUri, Lang.NQUADS, keepBNode, callback, parallel, strict);
			case RDFXML -> parse(input, baseUri, Lang.RDFXML, keepBNode, callback, parallel, strict);
			case N3, TURTLE -> parse(input, baseUri, Lang.TURTLE, keepBNode, callback, parallel, strict);
			case TRIG -> parse(input, baseUri, Lang.TRIG, keepBNode, callback, parallel, strict);
			case TRIX -> parse(input, baseUri, Lang.TRIX, keepBNode, callback, parallel, strict);
			default -> throw new NotImplementedException("Parser not found for format " + notation);
			}
		} catch (Exception e) {
			log.error("Unexpected exception.", e);
			throw new ParserException(e);
		}
	}

	private static class ElemStringBuffer implements StreamRDF {
		private final RDFCallback callback;

		private ElemStringBuffer(RDFCallback callback) {
			this.callback = callback;
		}

		@Override
		public void triple(Triple parsedTriple) {
			TripleString triple = new TripleString();
			triple.setAll(JenaNodeFormatter.format(parsedTriple.getSubject()),
					JenaNodeFormatter.format(parsedTriple.getPredicate()),
					JenaNodeFormatter.format(parsedTriple.getObject()));
			callback.processTriple(triple, 0);
		}

		@Override
		public void quad(Quad parsedQuad) {
			QuadString quad = new QuadString();
			quad.setAll(JenaNodeFormatter.format(parsedQuad.getSubject()),
					JenaNodeFormatter.format(parsedQuad.getPredicate()),
					JenaNodeFormatter.format(parsedQuad.getObject()), JenaNodeFormatter.format(parsedQuad.getGraph()));
			callback.processTriple(quad, 0);
		}

		@Override
		public void start() {
		}

		@Override
		public void base(String base) {
		}

		@Override
		public void prefix(String prefix, String iri) {
		}

		@Override
		public void finish() {
		}
	}
}
