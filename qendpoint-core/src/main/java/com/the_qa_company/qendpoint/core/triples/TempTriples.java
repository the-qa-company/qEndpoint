/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/triples
 * /TempTriples.java $ Revision: $Rev: 191 $ Last modified: $Date: 2013-03-03
 * 11:41:43 +0000 (dom, 03 mar 2013) $ Last modified by: $Author: mario.arias $
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
 * Martinez-Prieto: migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.triples;

import java.io.Closeable;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.dictionary.impl.DictionaryIDMapping;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;

/**
 * Interface for TempTriples implementation. This is a dynamic interface. For
 * static(read-only) behaviour have a look at {@link Triples}
 */
public interface TempTriples extends TriplesPrivate, Closeable {
	/**
	 * Add one triple
	 *
	 * @param subject   subject
	 * @param predicate predicate
	 * @param object    object
	 */
	boolean insert(long subject, long predicate, long object);

	/**
	 * Adds one or more triples
	 *
	 * @param triples The triples to be inserted
	 * @return boolean
	 */
	boolean insert(TripleID... triples);

	/**
	 * Deletes one or more triples according to a pattern
	 *
	 * @param pattern The pattern to match against
	 * @return boolean
	 */
	boolean remove(TripleID... pattern);

	/**
	 * Sorts the triples based on the order(TripleComponentOrder) of the
	 * triples. If you want to sort in a different order use setOrder first.
	 */
	void sort(ProgressListener listener);

	void removeDuplicates(ProgressListener listener);

	/**
	 * Sets a type of order(TripleComponentOrder)
	 *
	 * @param order The order to set
	 */
	void setOrder(TripleComponentOrder order);

	/**
	 * Clear all triples, resulting in an empty triples section.
	 */
	void clear();

	/**
	 * Load triples from another instance.
	 */
	void load(Triples triples, ProgressListener listener);

	void replaceAllIds(DictionaryIDMapping mapSubj, DictionaryIDMapping mapPred, DictionaryIDMapping mapObj);
}
