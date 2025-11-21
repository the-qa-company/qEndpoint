package com.the_qa_company.qendpoint.core.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;
import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap64Big;
import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.hdt.HDTVersion;
import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.rdf.RDFParserFactory;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import com.the_qa_company.qendpoint.core.util.listener.ColorTool;
import com.the_qa_company.qendpoint.core.util.listener.MultiThreadListenerConsole;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

public class HDTDiffCat {

	private ColorTool colorTool;

	@Parameter(description = "<input HDTs>+ <output HDT>")
	public List<String> parameters = Lists.newArrayList();

	@Parameter(names = "-options", description = "HDT Conversion options (override those of config file)")
	public String options;

	@Parameter(names = "-config", description = "Conversion config file")
	public String configFile;

	@Parameter(names = "-diff", description = "File to use to do the diff")
	public String diff;

	@Parameter(names = "-index", description = "Generate also external indices to solve all queries")
	public boolean generateIndex;

	@Parameter(names = "-version", description = "Prints the HDT version number")
	public static boolean showVersion;

	@Parameter(names = "-quiet", description = "Do not show progress of the conversion")
	public boolean quiet;

	@Parameter(names = "-color", description = "Print using color (if available)")
	public boolean color;

	private HDT diffcat(String location, HDTOptions spec, MultiThreadListener listener)
			throws IOException, ParserException, NotFoundException {
		List<String> inputs = parameters.subList(0, parameters.size() - 1);

		if (diff == null || diff.isEmpty()) {
			return HDTManager.catHDT(inputs, spec, listener);
		}

		RDFNotation type = RDFNotation.guess(diff);

		Bitmap64Big[] bms = new Bitmap64Big[inputs.size()];
		HDT[] inputsMap = new HDT[inputs.size()];

		try {
			for (int i = 0; i < bms.length; i++) {
				inputsMap[i] = HDTManager.mapHDT(inputs.get(i));
				bms[i] = Bitmap64Big.memory(inputsMap[i].getTriples().getNumberOfElements());
			}

			RDFParserCallback.RDFCallback callback = ((triple, pos) -> {
				for (int i = 0; i < inputsMap.length; i++) {
					IteratorTripleString find;
					try {
						find = inputsMap[i].search(triple);
					} catch (NotFoundException e) {
						throw new RuntimeException(e);
					}

					if (find.hasNext()) {
						find.next();
						bms[i].set(find.getLastTriplePosition(), true); // delete
																		// it
					}

				}
			});

			if (type == RDFNotation.HDT) {
				try (HDT diffHDT = HDTManager.mapHDT(diff)) {
					IteratorTripleString it = diffHDT.searchAll();
					while (it.hasNext()) {
						callback.processTriple(it.next(), 0);
					}
				}
			} else {
				RDFParserCallback parser = RDFParserFactory.getParserCallback(type, spec);
				parser.doParse(diff, "", type, true, callback, false);
			}
		} catch (Throwable t) {
			try {
				Closer.closeSingle(inputsMap);
			} catch (Throwable t2) {
				t.addSuppressed(t2);
			}
			throw t;
		}
		Closer.closeSingle(inputsMap);

		return HDTManager.diffBitCatHDT(inputs, List.of(bms), spec, listener);
	}

	public void execute() throws IOException, ParserException, NotFoundException {
		HDTOptions spec;
		if (configFile != null) {
			spec = HDTOptions.readFromFile(configFile);
		} else {
			spec = HDTOptions.of();
		}
		if (options != null) {
			spec.setOptions(options);
		}

		String hdtOutput = parameters.get(parameters.size() - 1);
		File file = new File(hdtOutput);

		String locationOpt = spec.get(HDTOptionsKeys.HDTCAT_LOCATION);

		if (locationOpt == null) {
			locationOpt = file.getAbsolutePath() + "_tmp";
			spec.set(HDTOptionsKeys.HDTCAT_LOCATION, locationOpt);
		}

		File theDir = new File(locationOpt);
		Files.createDirectories(theDir.toPath());
		String location = theDir.getAbsolutePath() + "/";

		MultiThreadListener listenerConsole = !quiet ? new MultiThreadListenerConsole(color) : null;
		StopWatch startCat = new StopWatch();
		try (HDT hdt = diffcat(location, spec, listenerConsole)) {
			colorTool.logValue("Files catdiff in ...... ", startCat.stopAndShow(), true);
			assert hdt != null;
			// Show Basic stats
			if (!quiet) {
				colorTool.logValue("Total Triples ......... ", String.valueOf(hdt.getTriples().getNumberOfElements()));
				colorTool.logValue("Different subjects .... ", String.valueOf(hdt.getDictionary().getNsubjects()));
				colorTool.logValue("Different predicates .. ", String.valueOf(hdt.getDictionary().getNpredicates()));
				colorTool.logValue("Different objects ..... ", String.valueOf(hdt.getDictionary().getNobjects()));
				colorTool.logValue("Common Subject/Object . ", String.valueOf(hdt.getDictionary().getNshared()));
			}

			// Dump to HDT file
			StopWatch sw = new StopWatch();
			hdt.saveToHDT(hdtOutput, listenerConsole);
			colorTool.logValue("HDT saved to file in .. ", sw.stopAndShow());
			Files.deleteIfExists(Path.of(location + "dictionary"));
			Files.deleteIfExists(Path.of(location + "triples"));
			FileUtils.deleteDirectory(theDir);

			// Generate index and dump it to .hdt.index file
			sw.reset();
			if (generateIndex) {
				HDTManager.indexedHDT(hdt, listenerConsole);
				colorTool.logValue("Index generated and saved in ", sw.stopAndShow());
			}
		}
	}

	public static void main(String[] args) throws Throwable {
		HDTDiffCat diffcat = new HDTDiffCat();
		JCommander com = new JCommander(diffcat);
		com.parse(args);
		com.setProgramName("hdtDiffCat");
		diffcat.colorTool = new ColorTool(diffcat.color, diffcat.quiet);

		if (showVersion) {
			diffcat.colorTool.log(HDTVersion.get_version_string("."));
			System.exit(0);
		} else if (diffcat.parameters.size() < 2) {
			com.usage();
			System.exit(1);
		}

		diffcat.colorTool.log("DiffCat "
				+ diffcat.parameters.stream().limit(diffcat.parameters.size() - 1).collect(Collectors.joining(", "))
				+ " to " + diffcat.parameters.get(diffcat.parameters.size() - 1));
		diffcat.execute();
	}
}
