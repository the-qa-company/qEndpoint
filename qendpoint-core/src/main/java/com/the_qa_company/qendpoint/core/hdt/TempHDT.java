/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/hdt/
 * TempHDT.java $ Revision: $Rev: 191 $ Last modified: $Date: 2013-03-03
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

package com.the_qa_company.qendpoint.core.hdt;

import com.the_qa_company.qendpoint.core.dictionary.TempDictionary;
import com.the_qa_company.qendpoint.core.header.Header;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.triples.TempTriples;

import java.io.Closeable;

/**
 * A TempHDT holds a TempDictionary and TempTriples that are used to gather the
 * information while building an HDT file. Allows adding and removing triples,
 * and operations to reorganize the internal information (Typically extracting
 * shared, sorting sections, removing duplicates...)
 *
 * @author mario.arias
 */
public interface TempHDT extends Closeable {

	void insert(CharSequence subject, CharSequence predicate, CharSequence object);

	/**
	 * This method should be used before reorganizing triples! It reorganizes
	 * the dictionary and updates it's ID's (usually done just by calling
	 * reorganize method of dictionary)
	 *
	 * @param listener
	 */
	void reorganizeDictionary(ProgressListener listener);

	/**
	 * It sorts the triples and removes duplicates.
	 *
	 * @param listener
	 */
	void reorganizeTriples(ProgressListener listener);

	boolean isOrganized();

	void clear();

	TempDictionary getDictionary();

	TempTriples getTriples();

	Header getHeader();

	String getBaseURI();
}
