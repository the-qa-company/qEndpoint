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

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.quad.QuadString;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.concurrent.ExceptionThread;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import org.apache.jena.graph.Triple;
import org.apache.jena.iri.impl.LexerFixer;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.lang.LabelToNode;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

/**
 * @author mario.arias
 */
public class RDFParserRIOT implements RDFParserCallback {
	private static final Logger log = LoggerFactory.getLogger(RDFParserRIOT.class);

	private static final int CORES = Runtime.getRuntime().availableProcessors();

	private void parse(InputStream stream, String baseUri, Lang lang, boolean keepBNode, ElemStringBuffer buffer,
			boolean parallel) {

		if (!parallel || (lang != Lang.NQUADS && lang != Lang.NTRIPLES)) {
			if (keepBNode) {
				RDFParser.source(stream).base(baseUri).lang(lang).labelToNode(LabelToNode.createUseLabelAsGiven())
						.parse(buffer);
			} else {
				RDFParser.source(stream).base(baseUri).lang(lang).parse(buffer);
			}
			return;
		}

		if (keepBNode) {
			LexerFixer.fixLexers();

			try (ConcurrentInputStream cs = new ConcurrentInputStream(stream, CORES - 1)) {
				ExceptionThread readThread = cs.getReadingThread();

				InputStream bnodes = cs.getBnodeStream();

				readThread.attach(new ExceptionThread(() -> RDFParser.source(bnodes).base(baseUri).lang(lang).labelToNode(LabelToNode.createUseLabelAsGiven())
						.parse(buffer), "BNode parser"));

				InputStream[] streams = cs.getStreams();
				int i = 0;
				for (InputStream s : streams) {
					readThread.attach(new ExceptionThread(() -> RDFParser.source(s).base(baseUri).lang(lang).labelToNode(LabelToNode.createUseLabelAsGiven())
							.parse(buffer), "Stream parser " + i));
				}
				try {
					readThread.joinAndCrashIfRequired();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			} catch (ExceptionThread.ExceptionThreadException e) {
				if (e.getCause() instanceof RuntimeException re) throw re;
				if (e.getCause() instanceof Error er) throw er;
				throw e;
			}
		} else {
			RDFParser.source(stream).base(baseUri).lang(lang).parse(buffer);
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
		try (InputStream input = IOUtil.getFileInputStream(fileName)) {
			doParse(input, baseUri, notation, keepBNode, callback, false);
		} catch (FileNotFoundException e) {
			throw new ParserException(e);
		} catch (Exception e) {
			log.error("Unexpected exception parsing file: {}", fileName, e);
			throw new ParserException(e);
		}
	}

	@Override
	public void doParse(InputStream input, String baseUri, RDFNotation notation, boolean keepBNode,
			RDFCallback callback, boolean parallel) throws ParserException {
		try {
			switch (notation) {
			case NTRIPLES -> parse(input, baseUri, Lang.NTRIPLES, keepBNode, new ElemStringBuffer(callback), parallel);
			case NQUAD -> parse(input, baseUri, Lang.NQUADS, keepBNode, new ElemStringBuffer(callback), parallel);
			case RDFXML -> parse(input, baseUri, Lang.RDFXML, keepBNode, new ElemStringBuffer(callback), parallel);
			case N3, TURTLE -> parse(input, baseUri, Lang.TURTLE, keepBNode, new ElemStringBuffer(callback), parallel);
			case TRIG -> parse(input, baseUri, Lang.TRIG, keepBNode, new ElemStringBuffer(callback), parallel);
			case TRIX -> parse(input, baseUri, Lang.TRIX, keepBNode, new ElemStringBuffer(callback), parallel);
			default -> throw new NotImplementedException("Parser not found for format " + notation);
			}
		} catch (Exception e) {
			log.error("Unexpected exception.", e);
			throw new ParserException(e);
		}
	}

	public static class ElemStringBuffer implements StreamRDF {
		private final RDFCallback callback;

		public ElemStringBuffer(RDFCallback callback) {
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
