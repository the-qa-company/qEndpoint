package com.the_qa_company.qendpoint.core.dictionary;
/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/
 * dictionary/TempDictionarySection.java $ Revision: $Rev: 191 $ Last modified:
 * $Date: 2013-03-03 11:41:43 +0000 (dom, 03 mar 2013) $ Last modified by:
 * $Author: mario.arias $ This library is free software; you can redistribute it
 * and/or modify it under the terms of the GNU Lesser General Public License as
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

import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.util.string.ByteString;

import java.util.Iterator;
import java.util.Map;

/**
 * Interface that specifies the methods for a dictionary section that can be
 * modified.
 *
 * @author Eugen
 */
public interface TempDictionarySection extends DictionarySection {

	/**
	 * Adds an entry to the dictionary section.
	 */
	long add(CharSequence str);

	/**
	 * Removes an entry from the dictionary section.
	 */
	void remove(CharSequence str);

	/**
	 * Sorts the entries in the dictionary section and updates their ID's
	 * according to the sorting order.
	 */
	void sort();

	/**
	 * Empties the dictionary section of all entries,
	 */
	void clear();

	/**
	 * Indicates if the sort method has been called and the section is sorted,
	 * ID's updated and the section ready to be used for generation of a
	 * QueryableDictionarySection.
	 */
	boolean isSorted();

	/**
	 * Iterator over all entries in the dictionary, not necessarily sorted.
	 */
	Iterator<? extends CharSequence> getEntries();

	/**
	 * @return the literal counts for MultipleSectionDictionary
	 */
	default Map<? extends ByteString, Long> getLiteralsCounts() {
		throw new NotImplementedException("getLiteralsCounts()");
	}

}
