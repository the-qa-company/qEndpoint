/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/enums/
 * RDFNotation.java $ Revision: $Rev: 17 $ Last modified: $Date: 2012-07-03
 * 21:43:15 +0100 (mar, 03 jul 2012) $ Last modified by: $Author:
 * mario.arias@gmail.com $ This library is free software; you can redistribute
 * it and/or modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation; either version 2.1 of the
 * License, or (at your option) any later version. This library is distributed
 * in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * library; if not, write to the Free Software Foundation, Inc., 51 Franklin St,
 * Fifth Floor, Boston, MA 02110-1301 USA Contacting the authors: Mario Arias:
 * mario.arias@deri.org Javier D. Fernandez: jfergar@infor.uva.es Miguel A.
 * Martinez-Prieto: migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.enums;

import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.rdf.parsers.RDFParserDir;
import com.the_qa_company.qendpoint.core.rdf.parsers.RDFParserHDT;
import com.the_qa_company.qendpoint.core.rdf.parsers.RDFParserList;
import com.the_qa_company.qendpoint.core.rdf.parsers.RDFParserRAR;
import com.the_qa_company.qendpoint.core.rdf.parsers.RDFParserRio;
import com.the_qa_company.qendpoint.core.rdf.parsers.RDFParserSimple;
import com.the_qa_company.qendpoint.core.rdf.parsers.RDFParserTar;
import com.the_qa_company.qendpoint.core.rdf.parsers.RDFParserZip;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Enumeration of the different valid notations for RDF data. It is used for
 * read/write operations by the core.
 */
@SuppressWarnings("unused")
public abstract class RDFNotation {
	private static final Map<String, RDFNotation> EXTENSIONS = new HashMap<>();
	private static final Map<RDFFormat, RDFNotation> RDF4J_FORMATS = new HashMap<>();

	public static RDFNotation fromRDF4J(RDFFormat format) {
		return RDF4J_FORMATS.computeIfAbsent(format, key -> new RDFNotationRDF4J(format));
	}

	public static RDFNotation of(Function<HDTOptions, RDFParserCallback> func, String name, String... extensions) {
		return new RDFNotationSupplier(func, name, extensions);
	}

	public static final RDFNotation RDFXML = fromRDF4J(RDFFormat.RDFXML);
	public static final RDFNotation NTRIPLES = new RDFNotation("ntriples", "nt") {
		@Override
		public RDFParserCallback createCallback(HDTOptions spec) {
			if (HDTOptions.ofNullable(spec).getBoolean(HDTOptionsKeys.NT_SIMPLE_PARSER_KEY, false)) {
				return new RDFParserSimple();
			}
			return new RDFParserRio();
		}

		@Override
		public RDFFormat asRDF4JFormat() {
			return RDFFormat.NTRIPLES;
		}

		@Override
		public String name() {
			return asRDF4JFormat().getName();
		}
	};
	public static final RDFNotation TURTLE = fromRDF4J(RDFFormat.TURTLE);
	public static final RDFNotation TRIG = fromRDF4J(RDFFormat.TRIG);
	public static final RDFNotation TRIX = fromRDF4J(RDFFormat.TRIX);
	public static final RDFNotation N3 = fromRDF4J(RDFFormat.N3);
	public static final RDFNotation TAR = of(RDFParserTar::new, "tar", "tgz", "tbz", "tbz2");
	public static final RDFNotation RAR = of(RDFParserRAR::new, "rar");
	public static final RDFNotation ZIP = of(RDFParserZip::new, "zip");
	public static final RDFNotation NQUAD = fromRDF4J(RDFFormat.NQUADS);
	public static final RDFNotation JSONLD = fromRDF4J(RDFFormat.JSONLD);
	public static final RDFNotation LIST = of(RDFParserList::new, "list");
	public static final RDFNotation DIR = of(RDFParserDir::new, "tar");
	public static final RDFNotation HDT = of(spec -> new RDFParserHDT(), "hdt");

	static {
		// set this custom object to add the simple parser option
		RDF4J_FORMATS.put(RDFFormat.NTRIPLES, NTRIPLES);
	}

	public static RDFNotation parse(String str) {
		if (str == null || str.isEmpty()) {
			return NTRIPLES;
		}
		RDFNotation notation = EXTENSIONS.get(str.toLowerCase());
		if (notation == null) {
			throw new IllegalArgumentException("Can't find notation for name: " + str);
		}
		return notation;
	}

	protected RDFNotation(String... extensions) {
		for (String extension : extensions) {
			EXTENSIONS.put(extension, this);
		}
	}

	public abstract RDFParserCallback createCallback(HDTOptions spec);

	public RDFFormat asRDF4JFormat() {
		throw new IllegalArgumentException("not an rdf4j valid format!");
	}

	public RDFParserCallback createCallback() {
		return createCallback(HDTOptions.EMPTY);
	}

	public static RDFNotation guess(String fileName) throws IllegalArgumentException {
		String str = fileName.toLowerCase();

		try {
			if (Files.isDirectory(Path.of(fileName))) {
				return DIR;
			}
		} catch (InvalidPathException e) {
			// not a valid path, so can't be a directory, ignore
		}

		int idx = str.lastIndexOf('.');
		if (idx != -1) {
			String ext = str.substring(idx + 1);
			if (ext.equals("gz") || ext.equals("bz") || ext.equals("bz2") || ext.equals("xz")) {
				str = str.substring(0, idx);
			}
		}

		if (str.endsWith("nt")) {
			return NTRIPLES;
		} else if (str.endsWith("n3")) {
			return N3;
		} else if (str.endsWith("nq") || str.endsWith("nquad")) {
			return NQUAD;
		} else if (str.endsWith("rdf") || str.endsWith("xml") || str.endsWith("owl")) {
			return RDFXML;
		} else if (str.endsWith("ttl")) {
			return TURTLE;
		} else if (str.endsWith("tar") || str.endsWith("tgz") || str.endsWith("tbz2")) {
			return TAR;
		} else if (str.endsWith("rar")) {
			return RAR;
		} else if (str.endsWith("zip")) {
			return ZIP;
		} else if (str.endsWith("list")) {
			return LIST;
		} else if (str.endsWith("hdt")) {
			return HDT;
		}

		throw new IllegalArgumentException("Could not guess the format for " + fileName);
	}

	public static RDFNotation guess(File fileName) throws IllegalArgumentException {
		return guess(fileName.getAbsolutePath());
	}

	public static RDFNotation guess(Path fileName) throws IllegalArgumentException {
		return guess(fileName.toAbsolutePath().toString());
	}

	public abstract String name();

	private static class RDFNotationRDF4J extends RDFNotation {
		private final RDFFormat format;

		private RDFNotationRDF4J(RDFFormat format) {
			super(format.getFileExtensions().toArray(String[]::new));
			this.format = format;
		}

		@Override
		public RDFParserCallback createCallback(HDTOptions spec) {
			return new RDFParserRio();
		}

		@Override
		public RDFFormat asRDF4JFormat() {
			return format;
		}

		@Override
		public String name() {
			return format.getName();
		}
	}

	private static class RDFNotationSupplier extends RDFNotation {
		private final Function<HDTOptions, RDFParserCallback> supplier;
		private final String name;

		private RDFNotationSupplier(Function<HDTOptions, RDFParserCallback> supplier, String name,
				String... extensions) {
			super(extensions);
			this.supplier = supplier;
			this.name = name;
		}

		@Override
		public RDFParserCallback createCallback(HDTOptions spec) {
			return supplier.apply(spec);
		}

		@Override
		public String name() {
			return name;
		}
	}
}
