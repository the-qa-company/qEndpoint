package com.the_qa_company.qendpoint.core.rdf.parsers;

import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.rdf.RDFParserFactory;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.io.NonCloseInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * Parses a tar file (optionally .tgz or .tar.gz or .tar.bz2) directly,
 * processing each file that contains rdf separately. It uses
 * RDFNotation.guess() to guess the format of each specific file. If not
 * recognised, each file of the tar is ignored.
 */

public class RDFParserTar implements RDFParserCallback {
	private static final Logger log = LoggerFactory.getLogger(RDFParserTar.class);
	private final HDTOptions spec;

	public RDFParserTar(HDTOptions spec) {
		this.spec = spec;
	}

	public RDFParserTar() {
		this(HDTOptions.EMPTY);
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.rdf.RDFParserCallback#doParse(java.lang.String,
	 * java.lang.String, hdt.enums.RDFNotation,
	 * hdt.rdf.RDFParserCallback.Callback)
	 */
	@Override
	public void doParse(String fileName, String baseUri, RDFNotation notation, boolean keepBNode, RDFCallback callback,
			boolean parallel) throws ParserException {
		try {
			InputStream input = IOUtil.getFileInputStream(fileName);
			this.doParse(input, baseUri, notation, keepBNode, callback, parallel);
			input.close();
		} catch (Exception e) {
			log.error("Unexpected exception parsing file: {}", fileName, e);
			throw new ParserException();
		}
	}

	@Override
	public void doParse(InputStream input, String baseUri, RDFNotation notation, boolean keepBNode,
			RDFCallback callback, boolean parallel) throws ParserException {
		try {

			final TarArchiveInputStream debInputStream = (TarArchiveInputStream) new ArchiveStreamFactory()
					.createArchiveInputStream("tar", input);
			TarArchiveEntry entry;

			// Make sure that the parser does not close the Tar Stream so we can
			// read the rest of the files.
			NonCloseInputStream nonCloseIn = new NonCloseInputStream(debInputStream);

			while ((entry = (TarArchiveEntry) debInputStream.getNextEntry()) != null) {

				if (entry.isFile() && !entry.getName().contains("DS_Store")) {
					try {
						RDFNotation guessnot = RDFNotation.guess(entry.getName());
						log.info("Parse from tar: {} as {}", entry.getName(), guessnot);
						RDFParserCallback parser = RDFParserFactory.getParserCallback(guessnot, spec);

						parser.doParse(nonCloseIn, baseUri, guessnot, keepBNode, callback, parallel);
					} catch (IllegalArgumentException | ParserException e1) {
						log.error("Unexpected exception.", e1);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new ParserException();
		}
	}

}
