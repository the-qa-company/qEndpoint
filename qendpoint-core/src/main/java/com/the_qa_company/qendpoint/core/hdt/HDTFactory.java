/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/hdt/
 * HDTFactory.java $ Revision: $Rev: 191 $ Last modified: $Date: 2013-03-03
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

import com.the_qa_company.qendpoint.core.hdt.impl.HDTImpl;
import com.the_qa_company.qendpoint.core.hdt.impl.ModeOfLoading;
import com.the_qa_company.qendpoint.core.hdt.impl.TempHDTImpl;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTSpecification;

import java.io.IOException;

/**
 * Factory that creates HDT objects
 */
public class HDTFactory {
	private HDTFactory() {
	}

	/**
	 * Creates a default HDT
	 *
	 * @return HDT
	 */
	public static HDT createHDT() throws IOException {
		return new HDTImpl(new HDTSpecification());
	}

	/**
	 * Creates an HDT with the specified spec
	 *
	 * @return HDT
	 */
	public static HDT createHDT(HDTOptions spec) throws IOException {
		return new HDTImpl(spec);
	}

	/**
	 * Creates a TempHDT with the specified spec, baseUri and ModeOfLoading.
	 * ModeOfLoading can be null if the TempHDT is not meant to be populated
	 * from RDF (i.e. ModHDTImporter object not used).
	 *
	 * @return TempHDT
	 */
	public static TempHDT createTempHDT(HDTSpecification spec, String baseUri, ModeOfLoading modeOfLoading) {
		return new TempHDTImpl(spec, baseUri, modeOfLoading);
	}
}
