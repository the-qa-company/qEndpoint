package com.the_qa_company.qendpoint.core.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.SameChecksumException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.listener.ColorTool;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class HDTAutoIndexer {
	private ColorTool colorTool;

	@Parameter(description = "<url/path> <output>")
	public List<String> parameters = new ArrayList<>();

	@Parameter(names = "-options", description = "HDT Conversion options (override those of config file)")
	public String options;

	@Parameter(names = "-config", description = "Conversion config file")
	public String configFile;

	@Parameter(names = "-index", description = "Generate also external indices to solve all queries")
	public boolean generateIndex;

	@Parameter(names = "-color", description = "Print using color (if available)")
	public boolean color;

	@Parameter(names = "-quiet", description = "Do not show progress of the conversion")
	public boolean quiet;

	@Parameter(names = "-base", description = "Base URI for the dataset")
	public String baseURI;

	public void execute() throws IOException, InterruptedException {
		HDTOptions cfg;
		if (configFile != null) {
			cfg = HDTOptions.readFromFile(configFile);
		} else {
			cfg = HDTOptions.of();
		}
		if (options != null) {
			cfg.setOptions(options);
		}

		String path = parameters.get(0);
		Path output = Path.of(parameters.get(1));

		if (baseURI == null) {
			String input = path.toLowerCase();
			if (input.startsWith("http") || input.startsWith("ftp")) {
				baseURI = URI.create(path).toString();
			} else {
				baseURI = Path.of(path).toUri().toString();
			}
			colorTool.warn("base uri not specified, using '" + baseURI + "'");
		}

		boolean one = !IOUtil.isRemoteURL(path) && cfg.getBoolean(HDTOptionsKeys.AUTOINDEXER_RUN_ONLY_REMOTE, true);
		colorTool.warn("The path is local, only one iteration will be performed, use "
				+ HDTOptionsKeys.AUTOINDEXER_RUN_ONLY_REMOTE + "=false to index it");

		HDTOptions spec = cfg.pushBottom();
		Path outTmpDir = output.resolve("tmp_output");
		Path checksumFile = outTmpDir.resolve("checksum");
		Path realChecksumFile = output.resolve("checksum");
		spec.setOptions(
				// predownload
				HDTOptionsKeys.LOADER_PREDOWNLOAD_URL, true,
				// temp file
				HDTOptionsKeys.LOADER_PREDOWNLOAD_URL_FILE, output.resolve("temp_dl" + IOUtil.getSuffix(path)),
				// checksum
				HDTOptionsKeys.LOADER_PREDOWNLOAD_CHECKSUM_PATH, checksumFile,
				// read checksum
				HDTOptionsKeys.LOADER_PREDOWNLOAD_CHECKSUM_FAIL_SAME, true);
		long sleepTime = spec.getInt(HDTOptionsKeys.AUTOINDEXER_SLEEP_TIME_MILLIS, 10_000);
		String hdtName = spec.get(HDTOptionsKeys.AUTOINDEXER_INDEX_NAME, "index_dev") + ".hdt";
		Path endIndex = output.resolve(hdtName);
		Path tmpIndex = outTmpDir.resolve(hdtName);
		Files.createDirectories(output);

		long currentId = 0;
		while (true) {
			StopWatch sw = new StopWatch();
			currentId++;
			colorTool.log("start iteration #" + currentId);
			colorTool.log("Cleaning old directory...");
			IOUtil.deleteDirRecurse(outTmpDir);
			Files.createDirectories(outTmpDir);
			if (!Files.exists(endIndex)) {
				colorTool.warn("No end hdt, deleting checksum file...");
				Files.deleteIfExists(realChecksumFile);
			} else if (Files.exists(realChecksumFile)) {
				Files.copy(realChecksumFile, checksumFile);
			}

			try {
				StopWatch sw2 = new StopWatch();
				try (HDT hdt = HDTManager.generateHDT(path, baseURI, RDFNotation.guess(path), spec,
						colorTool.getConsole())) {
					Files.createDirectories(tmpIndex.getParent());
					hdt.saveToHDT(tmpIndex);
					colorTool.log("HDT generated and saved in " + sw2.stopAndShow());

					sw2.reset();
					HDTManager.indexedHDT(hdt, colorTool.getConsole(), spec).close();
					colorTool.log("HDT indexes in " + sw2.stopAndShow());
				}

				try (Stream<Path> list = Files.list(outTmpDir)) {
					Iterator<Path> it = list.iterator();

					sw2.reset();
					while (it.hasNext()) {
						Path newFile = it.next();
						Path real = output.resolve(newFile.getFileName());

						colorTool.log("Moving " + newFile + " -> " + real);
						Files.move(newFile, real, StandardCopyOption.REPLACE_EXISTING);
					}
					colorTool.log("Moving done in " + sw2.stopAndShow());
				}
			} catch (SameChecksumException e) {
				colorTool.warn("Find the same checksum as the previous dataset, generation canceled");
			} catch (Exception e) {
				colorTool.error("Exception when running the tool");
				e.printStackTrace();
				System.err.println();
			}
			colorTool.log("iteration #" + currentId + " took " + sw.stopAndShow());
			IOUtil.deleteDirRecurse(outTmpDir);
			if (one) {
				colorTool.log("The path is local, no more iterations");
				break;
			}
			if (sleepTime > 0) {
				Thread.sleep(sleepTime);
			}

		}
	}

	public static void main(String[] args) throws Throwable {
		HDTAutoIndexer indexhdtrepo = new HDTAutoIndexer();
		JCommander com = new JCommander(indexhdtrepo);
		com.parse(args);
		com.setProgramName("indexhdtrepo");
		indexhdtrepo.colorTool = new ColorTool(indexhdtrepo.color, indexhdtrepo.quiet);

		if (indexhdtrepo.parameters.size() < 2) {
			com.usage();
			return;
		}

		indexhdtrepo.execute();
	}
}
