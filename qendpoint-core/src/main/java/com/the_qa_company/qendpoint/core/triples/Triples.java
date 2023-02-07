/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/iface/org/rdfhdt/hdt/
 * triples/Triples.java $ Revision: $Rev: 191 $ Last modified: $Date: 2013-03-03
 * 11:41:43 +0000 (dom, 03 mar 2013) $ Last modified by: $Author: mario.arias $
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version. This library is distributed in the hope that it will be
 * useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
 * General Public License for more details. You should have received a copy of
 * the GNU Lesser General Public License along with this library; if not, write
 * to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston,
 * MA 02110-1301 USA Contacting the authors: Mario Arias: mario.arias@deri.org
 * Javier D. Fernandez: jfergar@infor.uva.es Miguel A. Martinez-Prieto:
 * migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.triples;

import java.io.Closeable;

import com.the_qa_company.qendpoint.core.header.Header;

/**
 * Interface for Triples implementation.
 */
public interface Triples extends Closeable {
	/**
	 * Iterates over all triples. Equivalent to this.search(new TripleID());
	 *
	 * @return IteratorTripleID
	 */
	IteratorTripleID searchAll();

	/**
	 * Iterates over all triples that match the pattern.
	 *
	 * @param pattern The pattern to match against
	 * @return IteratorTripleID
	 */
	IteratorTripleID search(TripleID pattern);

	/**
	 * Returns the total number of triples
	 *
	 * @return int
	 */
	long getNumberOfElements();

	/**
	 * Returns the size in bytes of the internal representation
	 *
	 * @return int
	 */
	long size();

	/**
	 * Populates HDT Header with all information relevant to this Triples under
	 * a RDF root node.
	 *
	 * @param head     the header to populate
	 * @param rootNode the rdf root node to attach
	 */
	void populateHeader(Header head, String rootNode);

	/**
	 * Returns a unique identifier of this Triples Implementation
	 *
	 * @return String
	 */
	String getType();

	/**
	 * Find a triple with his position
	 *
	 * @param position The triple position
	 * @return triple
	 * @see IteratorTripleID#getLastTriplePosition()
	 * @see IteratorTripleString#getLastTriplePosition()
	 */
	TripleID findTriple(long position);
}
