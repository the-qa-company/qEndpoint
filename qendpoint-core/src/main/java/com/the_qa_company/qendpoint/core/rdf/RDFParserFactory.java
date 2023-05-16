/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/rdf/
 * RDFParserFactory.java $ Revision: $Rev: 191 $ Last modified: $Date:
 * 2013-03-03 11:41:43 +0000 (dom, 03 mar 2013) $ Last modified by: $Author:
 * mario.arias $ This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; version 3.0 of the License. This
 * library is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 * Contacting the authors: Mario Arias: mario.arias@deri.org Javier D.
 * Fernandez: jfergar@infor.uva.es Miguel A. Martinez-Prieto:
 * migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.rdf;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.rdf.parsers.RDFParserDir;
import com.the_qa_company.qendpoint.core.rdf.parsers.RDFParserHDT;
import com.the_qa_company.qendpoint.core.rdf.parsers.RDFParserList;
import com.the_qa_company.qendpoint.core.rdf.parsers.RDFParserRAR;
import com.the_qa_company.qendpoint.core.rdf.parsers.RDFParserRIOT;
import com.the_qa_company.qendpoint.core.rdf.parsers.RDFParserSimple;
import com.the_qa_company.qendpoint.core.rdf.parsers.RDFParserTar;
import com.the_qa_company.qendpoint.core.rdf.parsers.RDFParserZip;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.iterator.utils.PipedCopyIterator;

import java.io.InputStream;

/**
 * @author mario.arias
 */
public class RDFParserFactory {
	public static boolean useSimple(HDTOptions options) {
		return options != null && options.getBoolean(HDTOptionsKeys.NT_SIMPLE_PARSER_KEY, false);
	}

	public static RDFParserCallback getParserCallback(RDFNotation notation) {
		return getParserCallback(notation, HDTOptions.EMPTY);
	}

	public static RDFParserCallback getParserCallback(RDFNotation notation, HDTOptions spec) {
		switch (notation) {
		case NTRIPLES, NQUAD:
			if (useSimple(spec)) {
				return new RDFParserSimple();
			}
		case TURTLE:
		case N3:
		case RDFXML:
			return new RDFParserRIOT();
		case DIR:
			return new RDFParserDir(spec);
		case LIST:
			return new RDFParserList(spec);
		case ZIP:
			return new RDFParserZip(spec);
		case TAR:
			return new RDFParserTar(spec);
		case RAR:
			return new RDFParserRAR(spec);
		case HDT:
			return new RDFParserHDT();
		case JSONLD:
			// FIXME: Implement
			throw new NotImplementedException("RDFParserJSONLD not implemented");
		default:
			break;
		}

		throw new NotImplementedException("Parser not found for notation: " + notation);
	}

	/**
	 * convert a stream to a triple iterator
	 *
	 * @param parser   the parser to convert the stream
	 * @param stream   the stream to parse
	 * @param baseUri  the base uri to parse
	 * @param notation the rdf notation to parse
	 * @return iterator
	 */
	public static PipedCopyIterator<TripleString> readAsIterator(RDFParserCallback parser, InputStream stream,
			String baseUri, boolean keepBNode, RDFNotation notation) {
		return PipedCopyIterator.createOfCallback(pipe -> parser.doParse(stream, baseUri, notation, keepBNode,
				(triple, pos) -> pipe.addElement(triple.tripleToString())));
	}

}
