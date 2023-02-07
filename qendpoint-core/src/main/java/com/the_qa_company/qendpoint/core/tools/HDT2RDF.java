/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/tools/org/rdfhdt/hdt/tools
 * /HDT2RDF.java $ Revision: $Rev: 191 $ Last modified: $Date: 2013-03-03
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
package com.the_qa_company.qendpoint.core.tools;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.hdt.HDTVersion;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import com.the_qa_company.qendpoint.core.triples.TripleString;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;

/**
 * @author mario.arias
 */
public class HDT2RDF implements ProgressListener {
	@Parameter(description = "<input HDT> <output NT>")
	public List<String> parameters = Lists.newArrayList();

	@Parameter(names = "-version", description = "Prints the HDT version number")
	public static boolean showVersion;

	public String hdtInput;
	public String rdfOutput;

	public void execute() throws Exception {

		PrintStream out;
		if (rdfOutput.equals("stdout")) {
			out = System.out;
		} else {
			out = new PrintStream(rdfOutput, StandardCharsets.UTF_8);
		}

		try (HDT hdt = HDTManager.mapHDT(hdtInput, this)) {
			IteratorTripleString it = hdt.search("", "", "");
			StringBuilder build = new StringBuilder(1024);
			while (it.hasNext()) {
				TripleString triple = it.next();
				build.delete(0, build.length());
				triple.dumpNtriple(build);
				out.print(build);
			}
			if (!rdfOutput.equals("stdout")) {
				out.close();
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.ProgressListener#notifyProgress(float, java.lang.String)
	 */
	@Override
	public void notifyProgress(float level, String message) {
		// System.out.println(message + "\t"+ Float.toString(level));
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Throwable {
		HDT2RDF hdt2rdf = new HDT2RDF();
		JCommander com = new JCommander(hdt2rdf, args);
		com.setProgramName("hdt2rdf");

		if (showVersion) {
			System.out.println(HDTVersion.get_version_string("."));
			System.exit(0);
		}

		try {
			hdt2rdf.hdtInput = hdt2rdf.parameters.get(0);
		} catch (Exception e) {
			com.usage();
			System.exit(1);
		}

		try {
			hdt2rdf.rdfOutput = hdt2rdf.parameters.get(1);
		} catch (Exception e) {
			hdt2rdf.rdfOutput = "stdout";
		}
		System.err.println("Converting " + hdt2rdf.hdtInput + " to RDF on " + hdt2rdf.rdfOutput);

		hdt2rdf.execute();
	}

}
