/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/iface/org/rdfhdt/hdt/hdt/
 * HDT.java $ Revision: $Rev: 191 $ Last modified: $Date: 2013-03-03 11:41:43
 * +0000 (dom, 03 mar 2013) $ Last modified by: $Author: mario.arias $ This
 * library is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
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

package com.the_qa_company.qendpoint.core.hdt;

import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.header.Header;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.rdf.RDFAccess;
import com.the_qa_company.qendpoint.core.triples.Triples;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Interface that specifies the methods for a HDT implementation
 *
 * @author mario.arias
 */
public interface HDT extends RDFAccess, Closeable {

	/**
	 * Gets the header of the HDT
	 *
	 * @return Header
	 */
	Header getHeader();

	/**
	 * Gets the dictionary of the HDT
	 *
	 * @return Dictionary
	 */
	Dictionary getDictionary();

	/**
	 * Gets the triples of the HDT
	 *
	 * @return Triples
	 */
	Triples getTriples();

	/**
	 * Saves to OutputStream in HDT format
	 *
	 * @param output   The OutputStream to save to
	 * @param listener A listener that can be used to see the progress of the
	 *                 saving
	 * @throws IOException when the file cannot be found
	 */
	void saveToHDT(OutputStream output, ProgressListener listener) throws IOException;

	/**
	 * Saves to a file in HDT format
	 *
	 * @param fileName The OutputStream to save to
	 * @param listener A listener that can be used to see the progress of the
	 *                 saving
	 * @throws IOException when the file cannot be found
	 */
	void saveToHDT(String fileName, ProgressListener listener) throws IOException;

	/**
	 * Returns the size of the Data Structure in bytes.
	 *
	 * @return long
	 */
	long size();

	/**
	 * Get the Base URI for the Dataset.
	 *
	 * @return String
	 */
	String getBaseURI();

}
