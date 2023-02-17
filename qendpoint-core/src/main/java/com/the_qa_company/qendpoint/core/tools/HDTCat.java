package com.the_qa_company.qendpoint.core.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.hdt.HDTVersion;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import com.the_qa_company.qendpoint.core.util.listener.ColorTool;
import com.the_qa_company.qendpoint.core.util.listener.MultiThreadListenerConsole;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Dennis Diefenbach
 */
public class HDTCat implements ProgressListener {

	private ColorTool colorTool;

	@Parameter(description = "<input HDTs>+ <output HDT>")
	public List<String> parameters = Lists.newArrayList();

	@Parameter(names = "-options", description = "HDT Conversion options (override those of config file)")
	public String options;

	@Parameter(names = "-config", description = "Conversion config file")
	public String configFile;

	@Parameter(names = "-kcat", description = "Use KCat algorithm, default if the count of input HDTs isn't 2")
	public boolean kcat;

	@Parameter(names = "-index", description = "Generate also external indices to solve all queries")
	public boolean generateIndex;

	@Parameter(names = "-version", description = "Prints the HDT version number")
	public static boolean showVersion;

	@Parameter(names = "-quiet", description = "Do not show progress of the conversion")
	public boolean quiet;

	@Parameter(names = "-color", description = "Print using color (if available)")
	public boolean color;

	private HDT cat(String location, HDTOptions spec, ProgressListener listener) throws IOException {
		if (kcat) {
			return HDTManager.catHDT(parameters.subList(0, parameters.size() - 1), spec, listener);
		} else {
			return HDTManager.catHDT(location, parameters.get(0), parameters.get(1), spec, listener);
		}
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

		ProgressListener listenerConsole = !quiet ? (kcat ? new MultiThreadListenerConsole(color) : this) : null;
		StopWatch startCat = new StopWatch();
		try (HDT hdt = cat(location, spec, listenerConsole)) {
			colorTool.logValue("Files cat in .......... ", startCat.stopAndShow(), true);
			assert hdt != null;
			// Show Basic stats
			if (!quiet) {
				colorTool.logValue("Total Triples ......... ", "" + hdt.getTriples().getNumberOfElements());
				colorTool.logValue("Different subjects .... ", "" + hdt.getDictionary().getNsubjects());
				colorTool.logValue("Different predicates .. ", "" + hdt.getDictionary().getNpredicates());
				colorTool.logValue("Different objects ..... ", "" + hdt.getDictionary().getNobjects());
				colorTool.logValue("Common Subject/Object . ", "" + hdt.getDictionary().getNshared());
			}

			// Dump to HDT file
			StopWatch sw = new StopWatch();
			hdt.saveToHDT(hdtOutput, this);
			colorTool.logValue("HDT saved to file in .. ", sw.stopAndShow());
			Files.deleteIfExists(Path.of(location + "dictionary"));
			Files.deleteIfExists(Path.of(location + "triples"));
			FileUtils.deleteDirectory(theDir);

			// Generate index and dump it to .hdt.index file
			sw.reset();
			if (generateIndex) {
				HDTManager.indexedHDT(hdt, this);
				colorTool.logValue("Index generated and saved in ", sw.stopAndShow());
			}
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

	public static void main(String[] args) throws Throwable {
		HDTCat hdtCat = new HDTCat();
		JCommander com = new JCommander(hdtCat);
		com.parse(args);
		com.setProgramName("hdtCat");
		hdtCat.colorTool = new ColorTool(hdtCat.color, hdtCat.quiet);

		hdtCat.colorTool.log("Welcome to hdtCat!");
		hdtCat.colorTool.log("This tool was developed by Dennis Diefenbach and Jośe M. Giḿenez-Garćıa");

		if (showVersion) {
			hdtCat.colorTool.log(HDTVersion.get_version_string("."));
			System.exit(0);
		} else if (hdtCat.parameters.size() > 3) {
			// force k-cat if we have more than 2 HDTs to cat
			hdtCat.kcat = true;
		} else if (hdtCat.parameters.size() < 3) {
			com.usage();
			System.exit(1);
		}

		hdtCat.colorTool.log("Cat "
				+ hdtCat.parameters.stream().limit(hdtCat.parameters.size() - 1).collect(Collectors.joining(", "))
				+ " to " + hdtCat.parameters.get(hdtCat.parameters.size() - 1));
		hdtCat.execute();
	}
}
