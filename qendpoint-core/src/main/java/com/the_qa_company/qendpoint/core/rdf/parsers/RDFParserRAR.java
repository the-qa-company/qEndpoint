package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Parses a RAR file directly, processing each file that contains rdf
 * separately. It uses RDFNotation.guess() to guess the format of each specific
 * file. If not recognised, each file of the tar is ignored.
 */

public class RDFParserRAR implements RDFParserCallback {
	private static final Logger log = LoggerFactory.getLogger(RDFParserRAR.class);

	private final static String[] cmdList = { "unrar", "vb", "<RAR>" };
	private final static String[] cmdExtractFile = { "unrar", "p", "-inul", "<RAR>", "<FILE>" };
	private static Boolean available;
	private final HDTOptions spec;

	public RDFParserRAR(HDTOptions spec) {
		this.spec = spec;
	}

	public RDFParserRAR() {
		this(HDTOptions.EMPTY);
	}

	// List files in rar
//	unrar vb FILE.rar

	// Read a file
//	unrar p -inul FILE.rar path/to/file.txt

	public static boolean isAvailable() {
		if (available == null) {
			try {
				new ProcessBuilder(cmdList[0]).start();
				available = true;
			} catch (IOException e) {
				available = false;
			}
		}
		return available;
	}

	//

	// FIXME: Implements

	/*
	 * (non-Javadoc)
	 * @see hdt.rdf.RDFParserCallback#doParse(java.lang.String,
	 * java.lang.String, hdt.enums.RDFNotation,
	 * hdt.rdf.RDFParserCallback.Callback)
	 */
	@Override
	public void doParse(String rarFile, String baseUri, RDFNotation notation, boolean keepBNode, RDFCallback callback)
			throws ParserException {
		try {

			String[] cmdList1 = Arrays.copyOf(cmdList, cmdList.length);
			cmdList1[2] = rarFile;

			ProcessBuilder listProcessBuilder = new ProcessBuilder(cmdList1);
//			listProcess.redirectInput(tempFile);
			Process processList = listProcessBuilder.start();
			BufferedReader reader = new BufferedReader(new InputStreamReader(processList.getInputStream(), UTF_8));

			String[] cmdExtract = Arrays.copyOf(cmdExtractFile, cmdExtractFile.length);
			cmdExtract[3] = rarFile;

			String fileName;
			// Read file name from list
			while ((fileName = reader.readLine()) != null) {
				// FIXME: Create several processes in background?
				log.info("File: {}", fileName);
				RDFNotation guessnot = RDFNotation.guess(fileName);
				if (guessnot != null) {
					// Create
					log.info("Parse from rar: {} as {}", fileName, guessnot);
					RDFParserCallback parser = guessnot.createCallback(spec);

					cmdExtract[4] = fileName;
					ProcessBuilder extractProcessBuilder = new ProcessBuilder(cmdExtract);
					Process processExtract = extractProcessBuilder.start();

					InputStream in = processExtract.getInputStream();
					parser.doParse(in, baseUri, guessnot, keepBNode, callback);

					in.close();
					processExtract.waitFor();
				} else {
					log.info("Parse from rar {}: No suitable parser found.", fileName);
				}
			}

			reader.close();
			processList.waitFor();

		} catch (Exception e) {
			log.error("Unexpected exception parsing file: {}", rarFile, e);
			throw new ParserException();
		}
	}

	@Override
	public void doParse(InputStream input, String baseUri, RDFNotation notation, boolean keepBNode,
			RDFCallback callback) throws ParserException {
		throw new NotImplementedException();
	}

}
