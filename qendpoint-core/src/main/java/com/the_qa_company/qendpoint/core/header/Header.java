/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/iface/org/rdfhdt/hdt/
 * header/Header.java $ Revision: $Rev: 191 $ Last modified: $Date: 2013-03-03
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

package com.the_qa_company.qendpoint.core.header;

import java.io.IOException;
import java.io.OutputStream;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.ControlInfo;
import com.the_qa_company.qendpoint.core.rdf.RDFStorage;

/**
 * Provides the basic functionality for a header
 */
public interface Header extends RDFStorage {

	void save(OutputStream output, ControlInfo ci, ProgressListener listener) throws IOException;

	CharSequence getBaseURI();

	/**
	 * Returns the number of triples
	 *
	 * @return int
	 */
	int getNumberOfElements();
}
