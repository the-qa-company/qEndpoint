package com.the_qa_company.qendpoint.core.dictionary;
/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/iface/org/rdfhdt/hdt/
 * dictionary/Dictionary.java $ Revision: $Rev: 191 $ Last modified: $Date:
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

import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.header.Header;

import java.io.Closeable;
import java.util.Map;

/**
 * Interface that specifies the basic methods for any Dictionary implementation
 *
 * @author mario.arias, Eugen
 */
public interface Dictionary extends Closeable {

	/**
	 * Returns the string for a given id
	 *
	 * @param id       The id to convert to string
	 * @param position TriplePosition of the id in the dictionary
	 * @return String
	 */
	CharSequence idToString(long id, TripleComponentRole position);

	/**
	 * Returns the id for a given string
	 *
	 * @param str      The string to convert to id
	 * @param position TriplePosition of the string in the dictionary
	 * @return int
	 */
	long stringToId(CharSequence str, TripleComponentRole position);

	/**
	 * Returns the data type of a given literal string
	 *
	 * @param id The id to get the data type for
	 * @return String
	 */
	CharSequence dataTypeOfId(long id);

	/**
	 * Returns the number of elements in the dictionary
	 */
	long getNumberOfElements();

	/**
	 * Return the combined size of the sections of the dictionary (in bytes)
	 */
	long size();

	/**
	 * Returns the number of subjects in the dictionary. Note: Includes shared.
	 */
	long getNsubjects();

	/**
	 * Returns the number of predicates in the dictionary.
	 */
	long getNpredicates();

	/**
	 * Returns the number of objects in the dictionary. Note: Includes shared
	 */
	long getNobjects();

	/**
	 * Returns the number of subjects/objects in the dictionary.
	 */
	long getNAllObjects();

	long getNshared();

	DictionarySection getSubjects();

	DictionarySection getPredicates();

	DictionarySection getObjects();

	Map<? extends CharSequence, DictionarySection> getAllObjects();

	DictionarySection getShared();

	/**
	 * Fills the header with information from the dictionary
	 */
	void populateHeader(Header header, String rootNode);

	/**
	 * Returns the type of the dictionary (the way it is written onto file/held
	 * in memory)
	 *
	 * @return type
	 */
	String getType();
}
