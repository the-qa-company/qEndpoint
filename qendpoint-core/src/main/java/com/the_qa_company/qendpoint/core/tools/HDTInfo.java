package com.the_qa_company.qendpoint.core.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.the_qa_company.qendpoint.core.enums.CompressionType;
import com.the_qa_company.qendpoint.core.hdt.HDTVersion;
import com.the_qa_company.qendpoint.core.options.ControlInformation;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @author mario.arias
 */
public class HDTInfo {
	@Parameter(description = "<HDT File>")
	public List<String> parameters = new ArrayList<>();

	@Parameter(names = "-version", description = "Prints the HDT version number")
	public static boolean showVersion;

	public String hdtInput;

	public void execute() throws IOException {
		byte[] headerData;
		try (InputStream input = IOUtil.asUncompressed(new FileInputStream(hdtInput),
				CompressionType.guess(hdtInput))) {
			ControlInformation ci = new ControlInformation();

			// Load Global ControlInformation
			ci.load(input);

			// Load header
			ci.load(input);
			int headerSize = (int) ci.getInt("length");

			headerData = IOUtil.readBuffer(input, headerSize, null);
		}

		System.out.write(headerData, 0, headerData.length);
	}

	public static void main(String[] args) throws Throwable {
		HDTInfo hdtInfo = new HDTInfo();
		JCommander com = new JCommander(hdtInfo);
		com.parse(args);
		com.setProgramName("hdtInfo");
		if (showVersion) {
			System.out.println(HDTVersion.get_version_string("."));
			System.exit(0);
		}

		try {
			if (hdtInfo.hdtInput == null)
				hdtInfo.hdtInput = hdtInfo.parameters.get(0);
		} catch (Exception e) {
			com.usage();
			System.exit(1);
		}

		hdtInfo.execute();
	}

}
