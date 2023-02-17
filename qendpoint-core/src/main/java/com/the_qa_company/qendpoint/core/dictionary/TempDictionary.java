package com.the_qa_company.qendpoint.core.dictionary;
/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/
 * dictionary/TempDictionary.java $ Revision: $Rev: 191 $ Last modified: $Date:
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

import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.triples.TempTriples;

import java.io.Closeable;

/**
 * Interface that specifies the methods for a dictionary that can be modified.
 *
 * @author Eugen
 */
public interface TempDictionary extends Closeable {

	TempDictionarySection getSubjects();

	TempDictionarySection getPredicates();

	TempDictionarySection getObjects();

	TempDictionarySection getShared();

	/**
	 * To be executed at the start of the processing
	 */
	void startProcessing();

	/**
	 * To be executed at the end of the processing
	 */
	void endProcessing();

	/**
	 * Inserts a string in the dictionary in a position
	 *
	 * @param str      The string to be inserted
	 * @param position TriplePosition to be inserted in
	 */
	long insert(CharSequence str, TripleComponentRole position);

	/**
	 * Reorganizes the dictionary (Extract shared SO, sort sections). (used for
	 * two-pass way of work).
	 */
	void reorganize();

	/**
	 * Reorganizes the dictionary (Extract shared SO, sort sections) and updates
	 * the IDs of the triples (used for one-pass way of work).
	 */
	void reorganize(TempTriples triples);

	boolean isOrganized();

	/**
	 * Empty all the strings of the dictionary.
	 */
	void clear();

	/**
	 * Get the ID of a given String
	 *
	 * @param subject subject
	 * @param role    subject's role
	 */
	long stringToId(CharSequence subject, TripleComponentRole role);
}
