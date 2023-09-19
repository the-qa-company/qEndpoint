package com.the_qa_company.qendpoint.core.tools;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import com.the_qa_company.qendpoint.core.enums.CompressionType;
import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.hdt.HDTSupplier;
import com.the_qa_company.qendpoint.core.hdt.HDTVersion;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.rdf.RDFFluxStop;
import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.StopWatch;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;
import com.the_qa_company.qendpoint.core.util.StringUtil;
import com.the_qa_company.qendpoint.core.util.listener.ColorTool;
import com.the_qa_company.qendpoint.core.util.listener.MultiThreadListenerConsole;

/**
 * @author mario.arias
 */
public class RDF2HDT implements ProgressListener {
	/**
	 * @return a theoretical maximum amount of memory the JVM will attempt to
	 *         use
	 */
	private static long getMaxTreeCatChunkSize() {
		Runtime runtime = Runtime.getRuntime();
		return (long) ((runtime.maxMemory() - (runtime.totalMemory() - runtime.freeMemory())) / (0.85 * 5));
	}

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

	@Parameter(names = "-disk", description = "Generate the HDT on disk to reduce memory usage")
	public boolean disk;

	@Parameter(names = "-disklocation", description = "Location to run the generate disk, by default in a temporary directory, will be deleted after")
	public String diskLocation;

	@Parameter(names = "-canonicalntfile", description = "Only for NTriples input. Use a Fast NT file parser the input should be in a canonical form. See https://www.w3.org/TR/n-triples/#h2_canonical-ntriples")
	public boolean ntSimpleLoading;

	@Parameter(names = "-cattree", description = "Use HDTCatTree to split the HDT creation for big dataset")
	public boolean catTree;

	@Parameter(names = "-cattreelocation", description = "Only with -cattree, set the tree building location")
	public String catTreeLocation;

	@Parameter(names = "-multithread", description = "Use multithread logger")
	public boolean multiThreadLog;

	@Parameter(names = "-printoptions", description = "Print options")
	public boolean printoptions;

	@Parameter(names = "-color", description = "Print using color (if available)")
	public boolean color;

	private static long findBestMemoryChunkDiskMapTreeCat() {
		Runtime runtime = Runtime.getRuntime();
		long maxRam = (long) ((runtime.maxMemory() - (runtime.totalMemory() - runtime.freeMemory())) * 0.85) / 3;

		int shift = 0;

		while (shift != 63 && (1L << shift) * BitUtil.log2(1L << shift) < maxRam) {
			shift++;
		}

		// it will take at most "shift" bits per triple
		// we divide by 3 for the 3 maps
		return maxRam / shift;
	}

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

		RDFNotation notation = null;
		if (rdfType != null) {
			try {
				notation = RDFNotation.parse(rdfType);
			} catch (IllegalArgumentException e) {
				colorTool.warn("Notation " + rdfType + " not recognised.");
			}
		}

		if (notation == null) {
			try {
				notation = RDFNotation.guess(rdfInput);
			} catch (IllegalArgumentException e) {
				colorTool.warn("Could not guess notation for " + rdfInput + " Trying NTriples");
				notation = RDFNotation.NTRIPLES;
			}
		}

		boolean isQuad = notation == RDFNotation.NQUAD;

		if (isQuad) {
			if (!spec.contains(HDTOptionsKeys.TEMP_DICTIONARY_IMPL_KEY)) {
				spec.set(HDTOptionsKeys.TEMP_DICTIONARY_IMPL_KEY, HDTOptionsKeys.TEMP_DICTIONARY_IMPL_VALUE_HASH_QUAD);
			}
			if (!spec.contains(HDTOptionsKeys.DICTIONARY_TYPE_KEY)) {
				spec.set(HDTOptionsKeys.DICTIONARY_TYPE_KEY, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_QUAD_SECTION);
			}
		}

		colorTool.log("Converting " + rdfInput + " to " + hdtOutput + " as " + notation.name());

		if (ntSimpleLoading) {
			spec.set("parser.ntSimpleParser", "true");
		}

		StopWatch sw = new StopWatch();
		HDT hdt;

		if (catTree) {
			if (catTreeLocation != null) {
				spec.set("loader.cattree.location", catTreeLocation);
			}
			spec.set("loader.cattree.futureHDTLocation", hdtOutput);

			long maxTreeCatChunkSize = getMaxTreeCatChunkSize();

			colorTool.log("Compute HDT with HDTCatTree using chunk of size: "
					+ StringUtil.humanReadableByteCount(maxTreeCatChunkSize, true));

			if (disk) {
				if (diskLocation != null) {
					spec.set(HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, diskLocation);
					colorTool.log("Using temp directory " + diskLocation);
				}
				MultiThreadListenerConsole listenerConsole = !quiet ? new MultiThreadListenerConsole(color) : null;
				hdt = HDTManager.catTree(RDFFluxStop.countLimit(findBestMemoryChunkDiskMapTreeCat()),
						HDTSupplier.disk(), rdfInput, baseURI, notation, spec, listenerConsole);
				if (listenerConsole != null) {
					listenerConsole.notifyProgress(100, "done");
				}
			} else {
				hdt = HDTManager.catTree(RDFFluxStop.sizeLimit(maxTreeCatChunkSize), HDTSupplier.memory(), rdfInput,
						baseURI, notation, spec, this);
			}
		} else if (disk) {
			if (!quiet) {
				colorTool.log("Generating using generateHDTDisk");
			}
			spec.set(HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY, hdtOutput);
			if (diskLocation != null) {
				spec.set(HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, diskLocation);
				colorTool.log("Using temp directory " + diskLocation);
			}
			MultiThreadListenerConsole listenerConsole = !quiet ? new MultiThreadListenerConsole(color) : null;
			hdt = HDTManager.generateHDTDisk(rdfInput, baseURI, notation, CompressionType.guess(rdfInput), spec,
					listenerConsole);
			if (listenerConsole != null) {
				listenerConsole.notifyProgress(100, "done");
			}
		} else {
			ProgressListener listenerConsole = !quiet ? (multiThreadLog ? new MultiThreadListenerConsole(color) : this)
					: null;
			hdt = HDTManager.generateHDT(rdfInput, baseURI, notation, spec, listenerConsole);
		}

		colorTool.logValue("File converted in ..... ", sw.stopAndShow(), true);

		try {
			// Show Basic stats
			if (!quiet) {
				colorTool.logValue("Total Triples ......... ", String.valueOf(hdt.getTriples().getNumberOfElements()));
				colorTool.logValue("Different subjects .... ", String.valueOf(hdt.getDictionary().getNsubjects()));
				colorTool.logValue("Different predicates .. ", String.valueOf(hdt.getDictionary().getNpredicates()));
				colorTool.logValue("Different objects ..... ", String.valueOf(hdt.getDictionary().getNobjects()));
				if (isQuad)
					colorTool.logValue("Different graphs ...... ", String.valueOf(hdt.getDictionary().getNgraphs()));
				colorTool.logValue("Common Subject/Object . ", String.valueOf(hdt.getDictionary().getNshared()));
			}

			// Dump to HDT file
			if (!disk && !catTree) {
				sw = new StopWatch();
				hdt.saveToHDT(hdtOutput, this);
				colorTool.logValue("HDT saved to file in .. ", sw.stopAndShow());
			}

			// Generate index and dump it to .hdt.index file
			sw.reset();
			if (generateIndex) {
				hdt = HDTManager.indexedHDT(hdt, this);
				colorTool.logValue("Index generated and saved in ", sw.stopAndShow());
			}
		} finally {
			if (hdt != null)
				hdt.close();
		}

		// Debug all inserted triples
		// HdtSearch.iterate(hdt, "","","");
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
		RDF2HDT rdf2hdt = new RDF2HDT();
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
				case BOOLEAN:
					System.out.println(rdf2hdt.colorTool.color(3, 1, 5) + "Possible values: "
							+ rdf2hdt.colorTool.colorReset() + "true|false");
					break;
				case ENUM:
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
					break;
				default:
					break;
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
