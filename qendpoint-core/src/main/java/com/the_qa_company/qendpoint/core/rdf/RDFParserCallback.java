/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/iface/org/rdfhdt/hdt/rdf/
 * RDFParserCallback.java $ Revision: $Rev: 191 $ Last modified: $Date:
 * 2013-03-03 11:41:43 +0000 (dom, 03 mar 2013) $ Last modified by: $Author:
 * mario.arias $ This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License,
 * or (at your option) any later version. This library is distributed in the
 * hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * library; if not, write to the Free Software Foundation, Inc., 51 Franklin St,
 * Fifth Floor, Boston, MA 02110-1301 USA Contacting the authors: Mario Arias:
 * mario.arias@deri.org Javier D. Fernandez: jfergar@infor.uva.es Miguel A.
 * Martinez-Prieto: migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.rdf;

import java.io.InputStream;
import java.nio.file.Path;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.triples.TripleString;

/**
 * Parser that uses a callback to notify of each successfully parsed triple.
 *
 * @author mario.arias
 */
public interface RDFParserCallback {
	@FunctionalInterface
	interface RDFCallback {
		void processTriple(TripleString triple, long pos);

		/**
		 * @return an async version of this callback
		 */
		default RDFCallback async() {
			return ((triple, pos) -> {
				synchronized (this) {
					this.processTriple(triple, pos);
				}
			});
		}
	}

	void doParse(String fileName, String baseUri, RDFNotation notation, boolean keepBNode, RDFCallback callback)
			throws ParserException;

	default void doParse(Path file, String baseUri, RDFNotation notation, boolean keepBNode, RDFCallback callback)
			throws ParserException {
		doParse(file.toAbsolutePath().toString(), baseUri, notation, keepBNode, callback);
	}

	default void doParse(InputStream in, String baseUri, RDFNotation notation, boolean keepBNode, RDFCallback callback)
			throws ParserException {
		doParse(in, baseUri, notation, keepBNode, callback, false);
	};

	void doParse(InputStream in, String baseUri, RDFNotation notation, boolean keepBNode, RDFCallback callback,
			boolean parallel) throws ParserException;
}
