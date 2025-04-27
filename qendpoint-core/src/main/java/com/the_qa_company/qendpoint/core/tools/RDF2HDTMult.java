package com.the_qa_company.qendpoint.core.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.ChunkGenImpl;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.util.listener.ColorTool;
import com.the_qa_company.qendpoint.core.util.listener.MultiThreadListenerConsole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;

public class RDF2HDTMult implements ProgressListener {

	public String rdfInput;
	public String hdtOutput;

	private ColorTool colorTool;

	@Parameter(description = "<input RDF> <output HDT>")
	public List<String> parameters = Lists.newArrayList();

	@Parameter(names = "-options", description = "HDT Conversion options (override those of config file)")
	public String options;

	@Parameter(names = "-config", description = "Conversion config file")
	public String configFile;

	@Parameter(names = "-base", description = "Base URI for the dataset")
	public String baseURI;

	@Parameter(names = "-multithread", description = "Use multithread logger")
	public boolean multiThreadLog;

	@Parameter(names = "-color", description = "Print using color (if available)")
	public boolean color;

	public void execute() throws ParserException, IOException, ExecutionException, InterruptedException {
		HDTOptions spec;
		if (configFile != null) {
			spec = HDTOptions.readFromFile(configFile);
		} else {
			spec = HDTOptions.of();
		}
		if (options != null) {
			spec.setOptions(options);
		}

		List<Path> in;
		try (Stream<Path> list = Files.list(Path.of(rdfInput))) {
			in = list.toList();
		}

		Path outDir = Path.of(hdtOutput);
		Files.createDirectories(outDir);

		int workers = spec.getInt32(HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY, 0);
		int procs = spec.getInt32("rdf2hdtmult.numproc", () -> Runtime.getRuntime().availableProcessors());

		ProgressListener listenerConsole = (multiThreadLog ? new MultiThreadListenerConsole(color) : this);
		ChunkGenImpl gen = new ChunkGenImpl(Math.max(1, procs / (workers + 1)), spec, listenerConsole);

		List<Future<Path>> out = in.stream().map(path -> {
			if (baseURI == null) {
				baseURI = path.toUri().toString();
				colorTool.warn("base uri not specified, using '" + baseURI + "'");
			}
			Path outfile = outDir.resolve(path.getFileName() + ".hdt");
			return gen.genFile(path, outfile, baseURI, spec);
		}).toList();

		for (Future<Path> task : out) {
			Path join = task.get();
			System.out.println("Completed: " + join);
		}
		gen.crashAnyIo();

		System.out.println("all dataset completed");
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.ProgressListener#notifyProgress(float, java.lang.String)
	 */
	@Override
	public void notifyProgress(float level, String message) {
		System.out.print("\r" + message + "\t" + level + "                            \r");
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Throwable {
		RDF2HDTMult main = new RDF2HDTMult();
		JCommander com = new JCommander(main, args);
		com.setProgramName("rdf2hdtmult");
		main.colorTool = new ColorTool(main.color, false);

		if (main.parameters.size() == 1) {
			main.colorTool.warn("No input file specified, reading from standard input.");
			main.rdfInput = "-";
			main.hdtOutput = main.parameters.get(0);
		} else if (main.parameters.size() == 2) {
			main.rdfInput = main.parameters.get(0);
			main.hdtOutput = main.parameters.get(1);
		} else {
			com.usage();
			System.exit(1);
		}

		main.execute();
	}
}
