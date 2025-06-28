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

/**
 * @author mario.arias
 */
public class RDFParserSimple implements RDFParserCallback {
	private static final Logger log = LoggerFactory.getLogger(RDFParserSimple.class);

	/*
	 * (non-Javadoc)
	 * @see hdt.rdf.RDFParserCallback#doParse(java.lang.String,
	 * java.lang.String, hdt.enums.RDFNotation,
	 * hdt.rdf.RDFParserCallback.RDFCallback)
	 */
	@Override
	public void doParse(String fileName, String baseUri, RDFNotation notation, boolean keepBNode, RDFCallback callback,
			boolean parallel) throws ParserException {
		BufferedReader reader;
		try {
			reader = IOUtil.getFileReader(fileName);
		} catch (IOException e) {
			e.printStackTrace();
			throw new ParserException(e);
		}
		try {
			doParse(reader, baseUri, notation, keepBNode, callback);
		} finally {
			IOUtil.closeQuietly(reader);
		}
	}

	@Override
	public void doParse(InputStream input, String baseUri, RDFNotation notation, boolean keepBNode,
			RDFCallback callback, boolean parallel) throws ParserException {
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
}
