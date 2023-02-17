/**
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/rdf/parsers/RDFParserSimple.java
 * $ Revision: $Rev: 191 $ Last modified: $Date: 2013-03-03 11:41:43 +0000 (dom,
 * 03 mar 2013) $ Last modified by: $Author: mario.arias $ This library is free
 * software; you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation;
 * version 3.0 of the License. This library is distributed in the hope that it
 * will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 * of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
 * General Public License for more details. You should have received a copy of
 * the GNU Lesser General Public License along with this library; if not, write
 * to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston,
 * MA 02110-1301 USA Contacting the authors: Mario Arias: mario.arias@deri.org
 * Javier D. Fernandez: jfergar@infor.uva.es Miguel A. Martinez-Prieto:
 * migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author mario.arias
 */
public class RDFParserList implements RDFParserCallback {
	private final HDTOptions spec;

	public RDFParserList(HDTOptions spec) {
		this.spec = spec;
	}

	public RDFParserList() {
		this(HDTOptions.EMPTY);
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.rdf.RDFParserCallback#doParse(java.lang.String,
	 * java.lang.String, hdt.enums.RDFNotation,
	 * hdt.rdf.RDFParserCallback.RDFCallback)
	 */
	@Override
	public void doParse(String fileName, String baseUri, RDFNotation notation, boolean keepBNode, RDFCallback callback)
			throws ParserException {
		BufferedReader reader;
		try {
			reader = IOUtil.getFileReader(fileName);
		} catch (IOException e) {
			e.printStackTrace();
			throw new ParserException(e);
		}
		try {
			doParse(reader, baseUri, notation, keepBNode, callback);
		} finally {
			IOUtil.closeQuietly(reader);
		}
	}

	@Override
	public void doParse(InputStream input, String baseUri, RDFNotation notation, boolean keepBNode,
			RDFCallback callback) throws ParserException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		try {
			doParse(reader, baseUri, notation, keepBNode, callback);
		} finally {
			try {
				reader.close();
			} catch (IOException ignore) {
			}
		}
	}

	private void doParse(BufferedReader reader, String baseUri, RDFNotation notation, boolean keepBNode,
			RDFCallback callback) throws ParserException {
		try {
			String line;
			while ((line = reader.readLine()) != null) {
				line = line.trim();
				if (!line.startsWith("#")) {
					System.out.println(line);

					RDFNotation guessnot = RDFNotation.guess(line);
					System.out.println("Parse from list: " + line + " as " + guessnot);
					RDFParserCallback parser = guessnot.createCallback(spec);

					parser.doParse(line, baseUri, guessnot, keepBNode, callback);
				}
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new ParserException(e);
		}
	}

	public static List<String> getList(String file) throws IOException {
		BufferedReader reader = IOUtil.getFileReader(file);
		String line;
		ArrayList<String> list = new ArrayList<>();
		try {
			while ((line = reader.readLine()) != null) {
				line = line.trim();
				if (!line.startsWith("#")) {
					list.add(line);
				}
			}
		} finally {
			reader.close();
		}
		return list;
	}
}
