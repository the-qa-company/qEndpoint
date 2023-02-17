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
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author mario.arias
 */
public class RDFParserRio implements RDFParserCallback {
	private static final Logger log = LoggerFactory.getLogger(RDFParserRio.class);

	private void parse(InputStream stream, String baseUri, RDFNotation lang, boolean keepBNode) throws IOException {
		RDFParser parser = Rio.createParser(lang.asRDF4JFormat());
		parser.setRDFHandler(new RDFFileParser());
		parser.setPreserveBNodeIDs(keepBNode);
		parser.parse(stream, baseUri);
	}

	private RDFCallback callback;
	private final TripleString triple = new TripleString();

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
			doParse(input, baseUri, notation, keepBNode, callback);
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
		this.callback = callback;
		try {
			parse(input, baseUri, notation, keepBNode);
		} catch (Exception e) {
			log.error("Unexpected exception.", e);
			throw new ParserException(e);
		}
	}

	private class RDFFileParser implements RDFHandler {
		@Override
		public void startRDF() throws RDFHandlerException {
		}

		@Override
		public void endRDF() throws RDFHandlerException {
		}

		@Override
		public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
		}

		@Override
		public void handleStatement(Statement st) throws RDFHandlerException {
			triple.setAll(format(st.getSubject()), format(st.getPredicate()), format(st.getObject()));
			callback.processTriple(triple, 0);
		}

		public String format(Value node) {
			Values.iri("https://example.org/test");
			if (node.isIRI()) {
				IRI iri = (IRI) node;
				String ns = iri.getNamespace();
				if (ns.isEmpty()) {
					return iri.getLocalName();
				}
				return iri.getNamespace() + iri.getLocalName();

			} else if (node.isLiteral()) {
				Literal l = (Literal) node;
				IRI dt = l.getDatatype();
				if (dt == null || XSD.STRING.equals(dt)) {
					// String
					return '"' + node.stringValue() + '"';

				} else {
					String lang = l.getLanguage().orElse(null);
					if (lang != null) {
						// Lang. Lowercase the language tag to get semantic
						// equivalence
						// between "x"@en and "x"@EN as required by spec
						return '"' + node.stringValue() + "\"@" + lang.toLowerCase();

					} else {
						// Typed
						return '"' + node.stringValue() + "\"^^<" + dt.stringValue() + '>';
					}
				}

			} else if (node.isBNode()) {
				return "_:" + node.stringValue();

			} else {
				throw new IllegalArgumentException(String.valueOf(node));
			}
		}

		@Override
		public void handleComment(String comment) throws RDFHandlerException {
		}
	}
}
