/**
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/rdf/parsers/RDFParserSimple.java
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
 * migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.quad.QuadString;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * @author mario.arias
 */
public class RDFParserSimple implements RDFParserCallback {
	private static final Logger log = LoggerFactory.getLogger(RDFParserSimple.class);
	private final RDFParserRIOT riotParser = new RDFParserRIOT();
	private final boolean strict;

	public RDFParserSimple() {
		this(false);
	}

	public RDFParserSimple(boolean strict) {
		this.strict = strict;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.rdf.RDFParserCallback#doParse(java.lang.String,
	 * java.lang.String, hdt.enums.RDFNotation,
	 * hdt.rdf.RDFParserCallback.RDFCallback)
	 */
	@Override
	public void doParse(String fileName, String baseUri, RDFNotation notation, boolean keepBNode, RDFCallback callback)
			throws ParserException {
		try (InputStream input = IOUtil.getFileInputStream(fileName)) {
			doParse(input, baseUri, notation, keepBNode, callback);
		} catch (IOException e) {
			throw new ParserException(e);
		}
	}

	@Override
	public void doParse(InputStream input, String baseUri, RDFNotation notation, boolean keepBNode,
			RDFCallback callback) throws ParserException {
		if (notation == RDFNotation.NTRIPLES || notation == RDFNotation.NQUAD) {
			String parserBaseUri = baseUri;
			boolean hasBaseUri = baseUri != null && !baseUri.isEmpty();
			try (InputStream in = input) {
				RDFCallback strictCallback = (triple, pos) -> {
					if (!strict && hasBaseUri) {
						triple.setSubject(resolveRelativeIri(triple.getSubject(), baseUri));
						triple.setPredicate(resolveRelativeIri(triple.getPredicate(), baseUri));
						triple.setObject(resolveRelativeIri(triple.getObject(), baseUri));
						triple.setObject(resolveRelativeDatatypeIri(triple.getObject(), baseUri));
						if (triple instanceof QuadString quad) {
							quad.setGraph(resolveRelativeIri(quad.getGraph(), baseUri));
						}
					}
					if (strict) {
						requireAbsoluteIri(triple.getSubject(), "subject");
						requireAbsoluteIri(triple.getPredicate(), "predicate");
						requireAbsoluteIri(triple.getObject(), "object");
						requireAbsoluteDatatypeIri(triple.getObject());
						CharSequence graph = triple.getGraph();
						if (graph != null && graph.length() > 0) {
							requireAbsoluteIri(graph, "graph");
						}
					}
					callback.processTriple(triple, pos);
				};
				riotParser.doParse(in, parserBaseUri, notation, keepBNode, strictCallback, false, strict);
			} catch (IOException e) {
				throw new ParserException(e);
			}
			return;
		}
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
			doParse(reader, baseUri, notation, keepBNode, callback);
		} catch (IOException e) {
			throw new ParserException(e);
		}
	}

	private void doParse(BufferedReader reader, String baseUri, RDFNotation notation, boolean keepBNode,
			RDFCallback callback) throws ParserException {
		boolean readQuad = notation == RDFNotation.NQUAD;
		try (reader) {
			String line;
			long numLine = 1;
			TripleString triple;
			if (readQuad) {
				triple = new QuadString();
			} else {
				triple = new TripleString();
			}
			while ((line = reader.readLine()) != null) {
				// trim, find start
				int start = 0;
				while (start < line.length()) {
					char c = line.charAt(start);
					if (c != ' ' && c != '\t') {
						break;
					}
					start++;
				}
				// trim, find end
				int end = line.length() - 1;
				while (end >= 0) {
					char c = line.charAt(end);
					if (c != ' ' && c != '\t') {
						break;
					}
					end--;
				}
				// check that we have at least one element and this line isn't a
				// comment
				if (start + 1 < end && line.charAt(start) != '#') {
					triple.read(line, start, end, readQuad);
					if (!triple.hasEmpty()) {
						// System.out.println(triple);
						callback.processTriple(triple, 0);
					} else {
						log.warn("Could not parse triple at line " + numLine + ", ignored and not processed.\n" + line);
					}
				}
				numLine++;
			}
		} catch (Exception e) {
			log.error("Unexpected exception.", e);
			throw new ParserException(e);
		}
	}

	private static void requireAbsoluteIri(CharSequence value, String role) {
		if (!isIriToken(value)) {
			return;
		}
		if (!isAbsoluteIri(value)) {
			throw new IllegalArgumentException("Relative IRI not allowed for " + role + ": " + value);
		}
	}

	private static CharSequence resolveRelativeIri(CharSequence value, String baseUri) {
		if (!isIriToken(value) || isAbsoluteIri(value)) {
			return value;
		}
		String raw = value.toString();
		String iri = raw;
		int length = raw.length();
		if (length > 1 && raw.charAt(0) == '<' && raw.charAt(length - 1) == '>') {
			iri = raw.substring(1, length - 1);
		}
		if (isAbsoluteIri(iri)) {
			return value;
		}
		try {
			return URI.create(baseUri).resolve(iri).toString();
		} catch (IllegalArgumentException e) {
			return value;
		}
	}

	private static CharSequence resolveRelativeDatatypeIri(CharSequence literal, String baseUri) {
		if (literal == null || literal.length() == 0 || literal.charAt(0) != '"') {
			return literal;
		}
		int marker = datatypeMarkerIndex(literal);
		if (marker == -1) {
			return literal;
		}
		int start = marker + 3;
		int end = indexOfChar(literal, '>', start);
		if (end == -1) {
			return literal;
		}
		CharSequence iri = literal.subSequence(start, end);
		if (isAbsoluteIri(iri)) {
			return literal;
		}
		try {
			String resolved = URI.create(baseUri).resolve(iri.toString()).toString();
			StringBuilder updated = new StringBuilder(literal.length() + resolved.length());
			updated.append(literal, 0, start);
			updated.append(resolved);
			updated.append(literal, end, literal.length());
			return updated.toString();
		} catch (IllegalArgumentException e) {
			return literal;
		}
	}

	private static void requireAbsoluteDatatypeIri(CharSequence literal) {
		if (literal == null || literal.length() == 0 || literal.charAt(0) != '"') {
			return;
		}
		int marker = datatypeMarkerIndex(literal);
		if (marker == -1) {
			return;
		}
		int start = marker + 3;
		int end = indexOfChar(literal, '>', start);
		if (end == -1) {
			return;
		}
		CharSequence iri = literal.subSequence(start, end);
		if (!isAbsoluteIri(iri)) {
			throw new IllegalArgumentException("Relative datatype IRI not allowed: " + iri);
		}
	}

	private static boolean isIriToken(CharSequence value) {
		if (value == null || value.length() == 0) {
			return false;
		}
		char first = value.charAt(0);
		if (first == '"') {
			return false;
		}
		return !(first == '_' && value.length() > 1 && value.charAt(1) == ':');
	}

	private static int datatypeMarkerIndex(CharSequence literal) {
		int closingQuote = closingQuoteIndex(literal);
		if (closingQuote == -1) {
			return -1;
		}
		int marker = closingQuote + 1;
		if (marker + 2 >= literal.length()) {
			return -1;
		}
		if (literal.charAt(marker) == '^' && literal.charAt(marker + 1) == '^' && literal.charAt(marker + 2) == '<') {
			return marker;
		}
		return -1;
	}

	private static int closingQuoteIndex(CharSequence literal) {
		if (literal == null || literal.length() < 2 || literal.charAt(0) != '"') {
			return -1;
		}
		boolean escaped = false;
		for (int i = 1; i < literal.length(); i++) {
			char c = literal.charAt(i);
			if (escaped) {
				escaped = false;
				continue;
			}
			if (c == '\\') {
				escaped = true;
				continue;
			}
			if (c == '"') {
				return i;
			}
		}
		return -1;
	}

	private static int indexOfChar(CharSequence value, char token, int start) {
		for (int i = start; i < value.length(); i++) {
			if (value.charAt(i) == token) {
				return i;
			}
		}
		return -1;
	}

	private static boolean isAbsoluteIri(CharSequence iri) {
		int length = iri.length();
		int colon = -1;
		for (int i = 0; i < length; i++) {
			char c = iri.charAt(i);
			if (c == ':') {
				colon = i;
				break;
			}
			if (c == '/' || c == '?' || c == '#') {
				return false;
			}
		}
		if (colon <= 0) {
			return false;
		}
		if (!isAsciiAlpha(iri.charAt(0))) {
			return false;
		}
		for (int i = 1; i < colon; i++) {
			char c = iri.charAt(i);
			if (!(isAsciiAlpha(c) || isAsciiDigit(c) || c == '+' || c == '-' || c == '.')) {
				return false;
			}
		}
		return true;
	}

	private static boolean isAsciiAlpha(char c) {
		return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
	}

	private static boolean isAsciiDigit(char c) {
		return c >= '0' && c <= '9';
	}
}
