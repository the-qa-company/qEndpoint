package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.io.NonCloseInputStream;

import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Parses a tar file (optionally .tgz or .tar.gz or .tar.bz2) directly,
 * processing each file that contains rdf separately. It uses
 * RDFNotation.guess() to guess the format of each specific file. If not
 * recognised, each file of the tar is ignored.
 */

public class RDFParserZip implements RDFParserCallback {

	private final HDTOptions spec;

	public RDFParserZip(HDTOptions spec) {
		this.spec = spec;
	}

	public RDFParserZip() {
		this(HDTOptions.EMPTY);
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.rdf.RDFParserCallback#doParse(java.lang.String,
	 * java.lang.String, hdt.enums.RDFNotation,
	 * hdt.rdf.RDFParserCallback.Callback)
	 */
	@Override
	public void doParse(String fileName, String baseUri, RDFNotation notation, boolean keepBNode, RDFCallback callback)
			throws ParserException {
		try {
			InputStream input = IOUtil.getFileInputStream(fileName);
			this.doParse(input, baseUri, notation, keepBNode, callback);
			input.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new ParserException();
		}
	}

	@Override
	public void doParse(InputStream input, String baseUri, RDFNotation notation, boolean keepBNode,
			RDFCallback callback) throws ParserException {
		try {
			ZipInputStream zin = new ZipInputStream(input);

			// Don't allow the inner parser to close the InputStream to continue
			// reading entries.
			InputStream nonCloseIn = new NonCloseInputStream(zin);

			for (ZipEntry zipEntry; (zipEntry = zin.getNextEntry()) != null;) {
				if (!zipEntry.isDirectory()) {
					try {
						RDFNotation guessnot = RDFNotation.guess(zipEntry.getName());
						System.out.println("Parse from zip: " + zipEntry.getName() + " as " + guessnot);
						RDFParserCallback parser = guessnot.createCallback(spec);

						parser.doParse(nonCloseIn, baseUri, guessnot, keepBNode, callback);
					} catch (IllegalArgumentException | ParserException e1) {
						e1.printStackTrace();
					}
				}
			}
			// Don't close passed stream.

		} catch (Exception e) {
			e.printStackTrace();
			throw new ParserException(e);
		}
	}

}
