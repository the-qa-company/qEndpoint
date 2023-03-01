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
import com.the_qa_company.qendpoint.core.util.listener.ColorTool;
import com.the_qa_company.qendpoint.core.util.listener.MultiThreadListenerConsole;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

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

		try (HDT hdt = input(input, spec, listenerConsole)) {
			String oldType = hdt.getDictionary().getType();

			colorTool.log("find hdt of type: " + oldType);
			Converter converter;
			try {
				converter = Converter.newConverter(hdt, newType);
			} catch (IllegalArgumentException e) {
				colorTool.error(e.getMessage());
				return;
			}
			StopWatch watch = new StopWatch();
			converter.convertHDTFile(hdt, output, listenerConsole, spec);
			watch.stop();

			colorTool.log("Converted HDT to " + newType + " in " + watch + ".");
		}
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
