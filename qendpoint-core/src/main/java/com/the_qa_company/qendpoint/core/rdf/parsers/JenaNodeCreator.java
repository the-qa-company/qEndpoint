/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-jena/src/com/hp/hpl/jena/graph/
 * JenaNodeCreator.java $ Revision: $Rev: 190 $ Last modified: $Date: 2013-03-03
 * 11:30:03 +0000 (dom, 03 mar 2013) $ Last modified by: $Author: mario.arias $
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
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;

/**
 * @author mario.arias Creates Jena Nodes from HDT CharSequences formatted by
 *         {@link JenaNodeFormatter}.
 */
public class JenaNodeCreator {

	private JenaNodeCreator() {
	}

	public static Node create(CharSequence x) {
		String str = x.toString();

		if (str.length() == 0) {
			return createURI("");
		}

		char firstChar = str.charAt(0);

		if (firstChar == '_') {
			return createAnon(str);
		} else if (firstChar == '"') {
			return createLiteral(str);
		} else {
			return createURI(str);
		}
	}

	public static Node createAnon(String str) {
		return NodeFactory.createBlankNode(str.substring(2));
	}

	public static Node createLiteral(String str) {
		int len = str.length();

		String literal;
		String datatype;
		String lang;

		char next = '\0';
		for (int i = len - 1; i > 0; i--) {
			char cur = str.charAt(i);

			if (cur == '"') {
				if (next == '@') {
					literal = str.substring(1, i);
					lang = str.substring(i + 2, len);

					return NodeFactory.createLiteral(literal, lang);
				} else {
					literal = str.substring(1, i);

					return NodeFactory.createLiteral(literal);
				}
			} else if (cur == '^' && next == '^' && str.charAt(i - 1) == '"') {
				literal = str.substring(1, i - 1);

				if (str.charAt(i + 2) == '<' && str.charAt(len - 1) == '>') {
					datatype = str.substring(i + 3, len - 1);
				} else {
					datatype = str.substring(i + 2, len);
				}
				RDFDatatype rdfDataType = TypeMapper.getInstance().getSafeTypeByName(datatype);
				return NodeFactory.createLiteral(literal, rdfDataType);
			}

			next = cur;
		}
		// Note: this line is not reached unless the closing double quote is
		// missing
		return NodeFactory.createLiteral(str.substring(1, len - 2));
	}

	public static Node createURI(String str) {
		return NodeFactory.createURI(str);
	}
}
