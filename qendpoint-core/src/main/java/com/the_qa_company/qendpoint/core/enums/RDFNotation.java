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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;

/**
 * Enumeration of the different valid notations for RDF data. It is used for
 * read/write operations by Jena.
 */
public enum RDFNotation {
	/**
	 * XML notation
	 *
	 * @see <a href="http://en.wikipedia.org/wiki/RDF/XML">Wikipedia</a>
	 */
	RDFXML,
	/**
	 * N-TRIPLES notation
	 *
	 * @see <a href="http://en.wikipedia.org/wiki/N-Triples">Wikipedia</a>
	 */
	NTRIPLES,
	/**
	 * TURTLE notation
	 *
	 * @see <a href="http://en.wikipedia.org/wiki/Turtle_(syntax)">Wikipedia</a>
	 */
	TURTLE,
	/**
	 * Notation 3 notation
	 *
	 * @see <a href="http://en.wikipedia.org/wiki/Notation_3">Wikipedia</a>
	 */
	N3,

	/**
	 * Tar Package with multiple files in other RDF Formats
	 */
	TAR,

	/**
	 * RAR Package with multiple files in other RDF Formats
	 */
	RAR,

	/**
	 * ZIP Package with multiple files in other RDF Formats
	 */
	ZIP,

	/**
	 * NQuads
	 */
	NQUAD,

	/**
	 * JSON-LD
	 */
	JSONLD,

	/**
	 * List of URIs with RDF content in other RDF Formats
	 */
	LIST,

	/**
	 * Directory with RDF content
	 */
	DIR,

	/**
	 * HDT file
	 */
	HDT

	;

	public static RDFNotation parse(String str) {
		if (str == null || str.isEmpty()) {
			return NTRIPLES;
		}
		str = str.toLowerCase();
		switch (str) {
		case "ntriples":
		case "nt":
			return NTRIPLES;
		case "n3":
			return N3;
		case "nq":
		case "nquad":
			return NQUAD;
		case "rdfxml":
		case "rdf-xml":
		case "owl":
			return RDFXML;
		case "turtle":
			return TURTLE;
		case "rar":
			return RAR;
		case "tar":
		case "tgz":
		case "tbz":
		case "tbz2":
			return TAR;
		case "zip":
			return ZIP;
		case "list":
			return LIST;
		case "hdt":
			return HDT;
		}
		throw new IllegalArgumentException();
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
}
