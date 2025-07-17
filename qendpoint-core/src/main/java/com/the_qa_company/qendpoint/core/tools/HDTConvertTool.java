package com.the_qa_company.qendpoint.core.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.hdt.HDTVersion;
import com.the_qa_company.qendpoint.core.hdt.Converter;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import com.the_qa_company.qendpoint.core.util.io.IntegrityObject;
import com.the_qa_company.qendpoint.core.util.listener.ColorTool;
import com.the_qa_company.qendpoint.core.util.listener.MultiThreadListenerConsole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public class HDTConvertTool {

	private ColorTool colorTool;

	@Parameter(description = "<input HDT> <output HDT> <newType>")
	public List<String> parameters = Lists.newArrayList();

	@Parameter(names = "-options", description = "HDT Conversion options (override those of config file)")
	public String options;

	@Parameter(names = "-config", description = "Conversion config file")
	public String configFile;
	@Parameter(names = "-load", description = "load the inputHDT into memory")
	public boolean loadHDT;
	@Parameter(names = "-version", description = "Prints the HDT version number")
	public boolean showVersion;
	@Parameter(names = "-dict", description = "Prints the HDT dictionaries")
	public boolean showDictionaries;
	@Parameter(names = "-dir", description = "Use input and output as directories")
	public boolean dir;
	@Parameter(names = "-deleteBase", description = "delete the base, an integrity test will be done on the result")
	public boolean deleteBase;
	@Parameter(names = "-integrity", description = "use an integrity test on the result")
	public boolean integrity;

	@Parameter(names = "-quiet", description = "Do not show progress of the conversion")
	public boolean quiet;

	@Parameter(names = "-color", description = "Print using color (if available)")
	public boolean color;

	private HDT input(Path hdt, HDTOptions spec, ProgressListener listener) throws IOException {
		if (loadHDT) {
			return HDTManager.loadHDT(hdt, listener, spec);
		} else {
			return HDTManager.mapHDT(hdt, listener, spec);
		}
	}

	private record HDTConversionTask(Path input, Path output) {}

	private void integrityCheck(HDT oldHDT, Path newHDTPath, HDTOptions spec, ProgressListener listener)
			throws IOException {
		// check origin integrity
		try {
			IntegrityObject.checkObjectIntegrity(listener, oldHDT);
		} catch (IOException e) {
			throw new IOException("Invalid old hdt", e); // we need to add a
															// better msg
		}
		try (HDT newHDT = input(newHDTPath, spec, listener)) {
			try {
				IntegrityObject.checkObjectIntegrity(listener, newHDT);
			} catch (IOException e) {
				throw new IOException("Invalid new hdt", e);
			}
			if (newHDT.getTriples().getNumberOfElements() != oldHDT.getTriples().getNumberOfElements()) {
				throw new IOException("New and old HDTs don't contain the same amount of triples");
			}
			if (newHDT.getDictionary().getNumberOfElements() != oldHDT.getDictionary().getNumberOfElements()) {
				throw new IOException("New and old HDTs don't contain the same amount of dictionary elements");
			}
		}
	}

	private List<HDTConversionTask> getTasks(Path input, Path output) throws IOException {
		Path ina = input.toAbsolutePath();
		Path oua = output.toAbsolutePath();
		if (!dir) {
			// only two files
			return List.of(new HDTConversionTask(ina, oua));
		}

		try (Stream<Path> rec = Files.walk(ina)) {
			return rec.flatMap(f -> {
				Path in = f.toAbsolutePath();
				if (!in.toString().endsWith(".hdt")) {
					return Stream.empty(); // remove non hdt
				}
				Path out = oua.resolve(ina.relativize(in));
				return Stream.of(new HDTConversionTask(in, out));
			}).toList();
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

		Path input = Path.of(parameters.get(0));
		Path output = Path.of(parameters.get(1));
		String newType = parameters.get(2);

		if (input.toAbsolutePath().equals(output.toAbsolutePath())) {
			colorTool.error("Input = Output!");
			return;
		}

		ProgressListener listenerConsole;
		if (!quiet) {
			MultiThreadListenerConsole mtlc = new MultiThreadListenerConsole(color);
			colorTool.setConsole(mtlc);
			listenerConsole = mtlc;
		} else {
			listenerConsole = ProgressListener.ignore();
		}

		long total = 0;
		StopWatch gw = new StopWatch();
		List<HDTConversionTask> tasks = getTasks(input, output);
		for (HDTConversionTask task : tasks) {
			StopWatch lw = new StopWatch();
			colorTool.log("Converting " + task.input + " to " + task.output + "/" + newType + " " + (total + 1) + "/"
					+ tasks.size());
			try (HDT hdt = input(task.input, spec, listenerConsole)) {
				String oldType = hdt.getDictionary().getType();

				colorTool.log("find hdt of type: " + oldType + " in " + lw.stopAndShow());
				Converter converter;
				try {
					converter = Converter.newConverter(hdt, newType);
				} catch (IllegalArgumentException e) {
					colorTool.error("Can't create converter", e);
					break;
				}
				converter.convertHDTFile(hdt, task.output, listenerConsole, spec);
				colorTool.log("Converted HDT to " + newType + " in " + lw.stopAndShow() + ".");
				lw.reset();
				if (deleteBase || integrity) {
					integrityCheck(hdt, task.output, spec, listenerConsole);
					colorTool.log("Integrity test done in " + lw.stopAndShow() + ".");
				}
				total++;
			} catch (IOException e) {
				colorTool.error("Can't convert HDT", e);
				break;
			}
			if (deleteBase) {
				// the input/output were checked previously, now that the input
				// hdt is closed we can delete it.
				Files.deleteIfExists(task.input);
			}
		}
		colorTool.log("Converted " + total + "/" + tasks.size() + " HDT(s)  in " + gw.stopAndShow() + ".");
	}

	public static void main(String[] args) throws Throwable {
		HDTConvertTool hdtconvert = new HDTConvertTool();
		JCommander com = new JCommander(hdtconvert);
		com.parse(args);
		com.setProgramName("hdtconvert");
		hdtconvert.colorTool = new ColorTool(hdtconvert.color, hdtconvert.quiet);

		if (hdtconvert.showVersion) {
			hdtconvert.colorTool.log(HDTVersion.get_version_string("."));
			return;
		} else if (hdtconvert.showDictionaries) {
			HDTOptionsKeys.Option dict = HDTOptionsKeys.getOptionMap().get(HDTOptionsKeys.DICTIONARY_TYPE_KEY);
			List<HDTOptionsKeys.OptionValue> values = dict.getValues();
			hdtconvert.colorTool.log("Known dictionaries:");
			for (HDTOptionsKeys.OptionValue value : values) {
				hdtconvert.colorTool.log(hdtconvert.colorTool.color(5, 1, 0) + "`" + value.getValue() + "`"
						+ hdtconvert.colorTool.colorReset() + ": " + value.getValueInfo().desc());
			}
			return;
		} else if (hdtconvert.parameters.size() < 3) {
			com.usage();
			System.exit(1);
		}

		hdtconvert.execute();
	}
}
