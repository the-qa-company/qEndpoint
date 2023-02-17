package com.the_qa_company.qendpoint.core.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.hdt.HDTVersion;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import com.the_qa_company.qendpoint.core.util.UnicodeEscape;
import com.the_qa_company.qendpoint.core.util.listener.MultiThreadListenerConsole;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author mario.arias
 */
public class HdtSearch {
	@Parameter(description = "<HDT File>")
	public List<String> parameters = new ArrayList<>();

	@Parameter(names = "-options", description = "HDT Conversion options (override those of config file)")
	public String options;

	@Parameter(names = "-config", description = "Conversion config file")
	public String configFile;

	@Parameter(names = "-color", description = "Print using color (if available)")
	public boolean color;

	@Parameter(names = "-quiet", description = "Do not show progress of the conversion")
	public boolean quiet;

	@Parameter(names = "-version", description = "Prints the HDT version number")
	public static boolean showVersion;

	public String hdtInput;

	@Parameter(names = "-memory", description = "Load the whole file into main memory. Ensures fastest querying.")
	public boolean loadInMemory;

	protected static void iterate(HDT hdt, CharSequence subject, CharSequence predicate, CharSequence object)
			throws NotFoundException {
		StopWatch iterateTime = new StopWatch();
		int count;

		subject = subject.length() == 1 && subject.charAt(0) == '?' ? "" : subject;
		predicate = predicate.length() == 1 && predicate.charAt(0) == '?' ? "" : predicate;
		object = object.length() == 1 && object.charAt(0) == '?' ? "" : object;

		// Iterate over triples as Strings
		IteratorTripleString it = hdt.search(subject, predicate, object);
		count = 0;
		while (it.hasNext()) {
			TripleString triple = it.next();
			System.out.println(triple);
			count++;
		}

//		Iterate over triples only as IDs
//		TripleID patternID = DictionaryUtil.tripleStringtoTripleID(hdt.getDictionary(), new TripleString(subject, predicate, object));
//		IteratorTripleID it2 = hdt.getTriples().search(patternID);
//		while(it2.hasNext()) {
//			TripleID triple = it2.next();
//			System.out.println(triple);
//			count++;
//		}
		System.out.println("Iterated " + count + " triples in " + iterateTime.stopAndShow());
	}

	private void help() {
		System.out.println("HELP:");
		System.out.println("Please write Triple Search Pattern, using '?' for wildcards. e.g ");
		System.out.println("   http://www.somewhere.com/mysubject ? ?");
		System.out.println("Use 'exit' or 'quit' to terminate interactive shell.");
	}

	/**
	 * Read from a line, where each component is separated by space.
	 *
	 * @param line line to parse
	 */
	private static void parseTriplePattern(TripleString dest, String line) throws ParserException {
		int split, posa, posb;
		dest.clear();

		// SET SUBJECT
		posa = 0;
		posb = split = line.indexOf(' ', posa);

		if (posb == -1)
			throw new ParserException("Make sure that you included three terms."); // Not
																					// found,
																					// error.

		dest.setSubject(UnicodeEscape.unescapeString(line.substring(posa, posb)));

		// SET PREDICATE
		posa = split + 1;
		posb = split = line.indexOf(' ', posa);

		if (posb == -1)
			throw new ParserException("Make sure that you included three terms.");

		dest.setPredicate(UnicodeEscape.unescapeString(line.substring(posa, posb)));

		// SET OBJECT
		posa = split + 1;
		posb = line.length();

		if (line.charAt(posb - 1) == '.')
			posb--; // Remove trailing <space> <dot> from NTRIPLES.
		if (line.charAt(posb - 1) == ' ')
			posb--;

		dest.setObject(UnicodeEscape.unescapeString(line.substring(posa, posb)));
	}

	public void execute() throws IOException {
		HDTOptions spec;
		if (configFile != null) {
			spec = HDTOptions.readFromFile(configFile);
		} else {
			spec = HDTOptions.of();
		}
		if (options != null) {
			spec.setOptions(options);
		}

		ProgressListener listenerConsole = !quiet ? new MultiThreadListenerConsole(color) : ProgressListener.ignore();

		HDT hdt;
		if (loadInMemory) {
			hdt = HDTManager.loadIndexedHDT(hdtInput, listenerConsole, spec);
		} else {
			hdt = HDTManager.mapIndexedHDT(hdtInput, spec, listenerConsole);
		}

		BufferedReader in = new BufferedReader(new InputStreamReader(System.in, UTF_8));
		try {
			TripleString triplePattern = new TripleString();

			while (true) {
				System.out.print(">> ");
				System.out.flush();
				String line = in.readLine();
				if (line == null || line.equals("exit") || line.equals("quit")) {
					break;
				}
				if (line.equals("help")) {
					help();
					continue;
				}

				try {
					parseTriplePattern(triplePattern, line);
					System.out.println("Query: |" + triplePattern.getSubject() + "| |" + triplePattern.getPredicate()
							+ "| |" + triplePattern.getObject() + "|");

					iterate(hdt, triplePattern.getSubject(), triplePattern.getPredicate(), triplePattern.getObject());
				} catch (ParserException e) {
					System.err.println("Could not parse triple pattern: " + e.getMessage());
					help();
				} catch (NotFoundException e) {
					System.err.println("No results found.");
				}

			}
		} finally {
			if (hdt != null)
				hdt.close();
			in.close();
		}
	}

	public static void main(String[] args) throws Throwable {
		HdtSearch hdtSearch = new HdtSearch();
		JCommander com = new JCommander(hdtSearch);
		com.parse(args);
		com.setProgramName("hdtSearch");

		if (showVersion) {
			System.out.println(HDTVersion.get_version_string("."));
			System.exit(0);
		}

		if (hdtSearch.parameters.size() != 1) {
			com.usage();
			System.exit(1);
		}

		hdtSearch.hdtInput = hdtSearch.parameters.get(0);

		hdtSearch.execute();
	}

}
