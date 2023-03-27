package com.the_qa_company.qendpoint.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.the_qa_company.qendpoint.core.util.listener.ColorTool;
import com.the_qa_company.qendpoint.store.EndpointStoreUtils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

public class QueryFormatterTool {
	@Parameter(description = "<query>")
	public List<String> parameters = new ArrayList<>();
	@Parameter(names = "-color", description = "Print using color (if available)")
	public boolean color;

	@Parameter(names = "-quiet", description = "Do not show errors")
	public boolean quiet;

	public String input;

	private ColorTool colorTool;

	public void execute() {
		try {
			System.out.println(EndpointStoreUtils.formatSPARQLQuery(input));
		} catch (Exception e) {
			StringWriter writer = new StringWriter();
			e.printStackTrace(new PrintWriter(writer));
			colorTool.error(e.getMessage(), writer.toString(), false, true);
			System.exit(-1);
		}
	}

	public static void main(String[] args) throws Throwable {
		QueryFormatterTool formatterTool = new QueryFormatterTool();
		JCommander com = new JCommander(formatterTool);
		com.parse(args);
		com.setProgramName("queryFormat");

		formatterTool.colorTool = new ColorTool(formatterTool.color, formatterTool.quiet);

		if (formatterTool.parameters.size() == 0) {
			com.usage();
			System.exit(1);
		}

		formatterTool.input = String.join(" ", formatterTool.parameters);

		formatterTool.execute();
	}
}
