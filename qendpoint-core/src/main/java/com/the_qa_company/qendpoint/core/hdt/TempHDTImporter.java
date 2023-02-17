/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/hdt/
 * TempHDTImporter.java $ Revision: $Rev: 191 $ Last modified: $Date: 2013-03-03
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

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;

import java.io.IOException;

/**
 * Interface for classes that want to support loading data from RDF to a
 * TempHDT.
 *
 * @author mario.arias, Eugen
 */
public interface TempHDTImporter {

	TempHDT loadFromRDF(HDTOptions spec, String filename, String baseUri, RDFNotation notation,
			ProgressListener listener) throws IOException, ParserException;

}
