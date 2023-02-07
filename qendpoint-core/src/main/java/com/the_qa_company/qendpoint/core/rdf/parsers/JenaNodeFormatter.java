/*
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; version 3.0 of the License. This library is distributed
 * in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * library; if not, write to the Free Software Foundation, Inc., 51 Franklin St,
 * Fifth Floor, Boston, MA 02110-1301 USA Contacting the authors: Mario Arias:
 * mario.arias@deri.org Javier D. Fernandez: jfergar@infor.uva.es Miguel A.
 * Martinez-Prieto: migumar2@infor.uva.es
 */

package com.the_qa_company.qendpoint.core.rdf.parsers;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.datatypes.xsd.impl.RDFLangString;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.RDFNode;

/**
 * Converts a Jena {@link Node} to a String format that will round trip back to
 * the same Node via {@link JenaNodeCreator}. This does not conform to any
 * particular standard. In particular, there is no need to spend the time to
 * escape special characters within literal strings and {@link JenaNodeCreator}
 * does not unescape special characters.
 */
public class JenaNodeFormatter {

	private JenaNodeFormatter() {
	}

	public static String format(RDFNode n) {
		return format(n.asNode());
	}

	public static String format(Node node) {
		if (node.isURI()) {
			return node.getURI();

		} else if (node.isLiteral()) {
			RDFDatatype t = node.getLiteralDatatype();
			if (t == null || XSDDatatype.XSDstring.getURI().equals(t.getURI())) {
				// String
				return '"' + node.getLiteralLexicalForm() + '"';

			} else if (RDFLangString.rdfLangString.equals(t)) {
				// Lang. Lowercase the language tag to get semantic equivalence
				// between "x"@en and "x"@EN as required by spec
				return '"' + node.getLiteralLexicalForm() + "\"@" + node.getLiteralLanguage().toLowerCase();

			} else {
				// Typed
				return '"' + node.getLiteralLexicalForm() + "\"^^<" + t.getURI() + '>';
			}

		} else if (node.isBlank()) {
			return "_:" + node.getBlankNodeLabel();

		} else {
			throw new IllegalArgumentException(String.valueOf(node));
		}
	}
}
