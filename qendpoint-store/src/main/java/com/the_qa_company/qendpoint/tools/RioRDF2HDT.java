package com.the_qa_company.qendpoint.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;
import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.hdt.HDTVersion;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.listener.ColorTool;
import com.the_qa_company.qendpoint.core.util.listener.MultiThreadListenerConsole;
import com.the_qa_company.qendpoint.utils.RDFStreamUtils;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class RioRDF2HDT implements ProgressListener {

	public String rdfInput;
	public String hdtOutput;

	private ColorTool colorTool;

	@Parameter(description = "<input RDF> <output HDT>")
	public List<String> parameters = Lists.newArrayList();

	@Parameter(names = "-options", description = "HDT Conversion options (override those of config file)")
	public String options;

	@Parameter(names = "-config", description = "Conversion config file")
	public String configFile;

	@Parameter(names = "-rdftype", description = "Type of RDF Input (ntriples, nquad, n3, turtle, rdfxml)")
	public String rdfType;

	@Parameter(names = "-version", description = "Prints the HDT version number")
	public boolean showVersion;

	@Parameter(names = "-base", description = "Base URI for the dataset")
	public String baseURI;

	@Parameter(names = "-index", description = "Generate also external indices to solve all queries")
	public boolean generateIndex;

	@Parameter(names = "-quiet", description = "Do not show progress of the conversion")
	public boolean quiet;

	@Parameter(names = "-multithread", description = "Use multithread logger")
	public boolean multiThreadLog;

	@Parameter(names = "-printoptions", description = "Print options")
	public boolean printoptions;

	@Parameter(names = "-color", description = "Print using color (if available)")
	public boolean color;

	public void execute() throws ParserException, IOException {
		HDTOptions spec;
		if (configFile != null) {
			spec = HDTOptions.readFromFile(configFile);
		} else {
			spec = HDTOptions.of();
		}
		if (options != null) {
			spec.setOptions(options);
		}
		if (baseURI == null) {
			String input = rdfInput.toLowerCase();
			if (input.startsWith("http") || input.startsWith("ftp")) {
				baseURI = URI.create(rdfInput).toString();
			} else {
				baseURI = Path.of(rdfInput).toUri().toString();
			}
			colorTool.warn("base uri not specified, using '" + baseURI + "'");
		}

		RDFFormat notation = null;
		if (rdfType != null) {
			try {
				notation = RDFStreamUtils.hdtToRDF4JNotation(RDFNotation.parse(rdfType));
			} catch (IllegalArgumentException e) {
				colorTool.warn("Notation " + rdfType + " not recognised.");
			}
		}

		if (notation == null) {
			try {
				notation = RDFStreamUtils.hdtToRDF4JNotation(RDFNotation.guess(rdfInput));
			} catch (IllegalArgumentException e) {
				colorTool.warn("Could not guess notation for " + rdfInput + " Trying NTriples");
				notation = RDFFormat.NTRIPLES;
			}
		}

		boolean isQuad = notation == RDFFormat.NQUADS;

		if (isQuad) {
			if (!spec.contains(HDTOptionsKeys.TEMP_DICTIONARY_IMPL_KEY)) {
				spec.set(HDTOptionsKeys.TEMP_DICTIONARY_IMPL_KEY, HDTOptionsKeys.TEMP_DICTIONARY_IMPL_VALUE_HASH_QUAD);
			}
			if (!spec.contains(HDTOptionsKeys.DICTIONARY_TYPE_KEY)) {
				spec.set(HDTOptionsKeys.DICTIONARY_TYPE_KEY, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_QUAD_SECTION);
			}
		}

		colorTool.log("Converting " + rdfInput + " to " + hdtOutput + " as " + notation.getName());

		StopWatch sw = new StopWatch();
		ProgressListener listenerConsole = !quiet ? (multiThreadLog ? new MultiThreadListenerConsole(color) : this)
				: null;

		HDT hdt = null;
		try (InputStream is = new BufferedInputStream(Files.newInputStream(Path.of(rdfInput)))) {
			Iterator<TripleString> it = RDFStreamUtils.readRDFStreamAsTripleStringIterator(is, notation, true);

			hdt = HDTManager.generateHDT(it, baseURI, spec, listenerConsole);

			colorTool.logValue("File converted in ..... ", sw.stopAndShow(), true);

			// Show Basic stats
			if (!quiet) {
				colorTool.logValue("Total Triples ......... ", String.valueOf(hdt.getTriples().getNumberOfElements()));
				colorTool.logValue("Different subjects .... ", String.valueOf(hdt.getDictionary().getNsubjects()));
				colorTool.logValue("Different predicates .. ", String.valueOf(hdt.getDictionary().getNpredicates()));
				colorTool.logValue("Different objects ..... ", String.valueOf(hdt.getDictionary().getNobjects()));
				if (hdt.getDictionary().supportGraphs()) {
					colorTool.logValue("Different graphs ...... ", String.valueOf(hdt.getDictionary().getNgraphs()));
				}
				colorTool.logValue("Common Subject/Object . ", String.valueOf(hdt.getDictionary().getNshared()));
			}

			// Dump to HDT file
			sw.reset();
			hdt.saveToHDT(hdtOutput, this);
			colorTool.logValue("HDT saved to file in .. ", sw.stopAndShow());

			// Generate index and dump it to .hdt.index file
			sw.reset();
			if (generateIndex) {
				hdt = HDTManager.indexedHDT(hdt, this);
				colorTool.logValue("Index generated and saved in ", sw.stopAndShow());
			}
		} finally {
			IOUtil.closeObject(hdt);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.ProgressListener#notifyProgress(float, java.lang.String)
	 */
	@Override
	public void notifyProgress(float level, String message) {
		if (!quiet) {
			System.out.print("\r" + message + "\t" + level + "                            \r");
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Throwable {
		RioRDF2HDT rdf2hdt = new RioRDF2HDT();
		JCommander com = new JCommander(rdf2hdt, args);
		com.setProgramName("rdf2hdt");
		rdf2hdt.colorTool = new ColorTool(rdf2hdt.color, rdf2hdt.quiet);

		if (rdf2hdt.printoptions) {
			Collection<HDTOptionsKeys.Option> values = HDTOptionsKeys.getOptionMap().values();

			for (HDTOptionsKeys.Option opt : values) {
				System.out.println(
						rdf2hdt.colorTool.color(3, 1, 5) + "Key:  " + rdf2hdt.colorTool.color(5, 1, 0) + opt.getKey());
				if (!opt.getKeyInfo().desc().isEmpty()) {
					System.out.println(rdf2hdt.colorTool.color(3, 1, 5) + "Desc: " + rdf2hdt.colorTool.colorReset()
							+ opt.getKeyInfo().desc());
				}
				System.out.println(rdf2hdt.colorTool.color(3, 1, 5) + "Type: " + rdf2hdt.colorTool.colorReset()
						+ opt.getKeyInfo().type().getTitle());
				switch (opt.getKeyInfo().type()) {
				case BOOLEAN -> System.out.println(rdf2hdt.colorTool.color(3, 1, 5) + "Possible values: "
						+ rdf2hdt.colorTool.colorReset() + "true|false");
				case ENUM -> {
					System.out.println(rdf2hdt.colorTool.color(3, 1, 5) + "Possible value(s):");
					int max = opt.getValues().stream().mapToInt(vle -> vle.getValue().length()).max().orElse(0);
					for (HDTOptionsKeys.OptionValue vle : opt.getValues()) {
						System.out.print(rdf2hdt.colorTool.color(3, 3, 3) + "- " + rdf2hdt.colorTool.colorReset()
								+ vle.getValue());
						if (!vle.getValueInfo().desc().isEmpty()) {
							System.out.println(rdf2hdt.colorTool.color(3, 3, 3)
									+ " ".repeat(max - vle.getValue().length()) + " : " + vle.getValueInfo().desc());
						} else {
							System.out.println();
						}
					}
				}
				default -> {
				}
				}
				System.out.println("\n");
			}

			return;
		}

		if (rdf2hdt.parameters.size() == 1) {
			rdf2hdt.colorTool.warn("No input file specified, reading from standard input.");
			rdf2hdt.rdfInput = "-";
			rdf2hdt.hdtOutput = rdf2hdt.parameters.get(0);
		} else if (rdf2hdt.parameters.size() == 2) {
			rdf2hdt.rdfInput = rdf2hdt.parameters.get(0);
			rdf2hdt.hdtOutput = rdf2hdt.parameters.get(1);
		} else if (rdf2hdt.showVersion) {
			System.out.println(HDTVersion.get_version_string("."));
			System.exit(0);
		} else {
			com.usage();
			System.exit(1);
		}

		rdf2hdt.execute();
	}
}
