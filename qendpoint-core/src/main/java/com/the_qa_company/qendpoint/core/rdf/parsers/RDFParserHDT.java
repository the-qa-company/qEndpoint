package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author Antoine Willerval
 */
public class RDFParserHDT implements RDFParserCallback {
	private static final Logger log = LoggerFactory.getLogger(RDFParserHDT.class);

	@Override
	public void doParse(String fileName, String baseUri, RDFNotation notation, boolean keepBNode, RDFCallback callback)
			throws ParserException {
		try (HDT hdt = HDTManager.mapHDT(fileName)) {
			hdt.search("", "", "").forEachRemaining(t -> callback.processTriple(t, 0));
		} catch (IOException | NotFoundException e) {
			log.error("Unexpected exception.", e);
			throw new ParserException(e);
		}
	}

	@Override
	public void doParse(InputStream in, String baseUri, RDFNotation notation, boolean keepBNode, RDFCallback callback,
			boolean parallel) throws ParserException {
		try {
			// create a temp
			Path tempFile = Files.createTempFile("hdtjava-reader", ".hdt");
			log.warn("Create temp file to store the HDT stream {}", tempFile);
			try {
				Files.copy(in, tempFile);
				doParse(tempFile.toAbsolutePath().toString(), baseUri, notation, keepBNode, callback);
			} finally {
				Files.deleteIfExists(tempFile);
			}
		} catch (IOException e) {
			log.error("Unexpected exception.", e);
			throw new ParserException(e);
		}
	}

}
