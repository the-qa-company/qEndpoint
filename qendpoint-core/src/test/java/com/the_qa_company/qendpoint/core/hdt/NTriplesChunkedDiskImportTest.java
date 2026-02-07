package com.the_qa_company.qendpoint.core.hdt;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.enums.CompressionType;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;

import static org.junit.Assert.assertEquals;

public class NTriplesChunkedDiskImportTest {
	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	@Test
	public void generateDiskWithSimpleParserUsesPullChunking() throws IOException, ParserException {
		Path root = tempDir.newFolder().toPath();
		Path nt = root.resolve("data.nt");

		Files.writeString(nt,
				"<http://ex/s> <http://ex/p> \"o\" .\n" + "<http://ex/s2> <http://ex/p2> <http://ex/o2> .\n");

		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.NT_SIMPLE_PARSER_KEY, "true",
				HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, root.resolve("work").toAbsolutePath().toString(),
				HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY, "2", HDTOptionsKeys.LOADER_DISK_CHUNK_SIZE_KEY, "1");

		try (HDT hdt = HDTManager.generateHDTDisk(nt.toString(), "http://ex/", RDFNotation.NTRIPLES, spec, null)) {
			assertEquals(2, hdt.getTriples().getNumberOfElements());
		}
	}

	@Test
	public void generateDiskWithSimpleParserSupportsGzip() throws IOException, ParserException {
		Path root = tempDir.newFolder().toPath();
		Path ntGz = root.resolve("data.nt.gz");

		byte[] content = ("<http://ex/s> <http://ex/p> \"o\" .\n" + "<http://ex/s2> <http://ex/p2> <http://ex/o2> .\n")
				.getBytes(StandardCharsets.UTF_8);
		try (OutputStream out = new GZIPOutputStream(Files.newOutputStream(ntGz))) {
			out.write(content);
		}

		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.NT_SIMPLE_PARSER_KEY, "true",
				HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, root.resolve("work").toAbsolutePath().toString(),
				HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY, "2", HDTOptionsKeys.LOADER_DISK_CHUNK_SIZE_KEY, "1");

		try (HDT hdt = HDTManager.generateHDTDisk(ntGz.toString(), "http://ex/", RDFNotation.NTRIPLES,
				CompressionType.GZIP, spec, null)) {
			assertEquals(2, hdt.getTriples().getNumberOfElements());
		}
	}

	@Test
	public void generateWithSimpleParserUsesPullChunkingForStreamInput() throws IOException, ParserException {
		Path root = tempDir.newFolder().toPath();

		byte[] content = ("<http://ex/s> <http://ex/p> \"o\"\n").getBytes(StandardCharsets.UTF_8);

		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.NT_SIMPLE_PARSER_KEY, "true", HDTOptionsKeys.LOADER_TYPE_KEY,
				HDTOptionsKeys.LOADER_TYPE_VALUE_DISK, HDTOptionsKeys.LOADER_DISK_LOCATION_KEY,
				root.resolve("work").toAbsolutePath().toString(), HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY,
				"2", HDTOptionsKeys.LOADER_DISK_CHUNK_SIZE_KEY, "1");

		try (InputStream in = new ByteArrayInputStream(content);
				HDT hdt = HDTManager.generateHDT(in, "http://ex/", "data.nt", spec, null)) {
			assertEquals(1, hdt.getTriples().getNumberOfElements());
		}
	}

	@Test
	public void generateWithSimpleParserUsesPullChunkingForGzipStreamInput() throws IOException, ParserException {
		Path root = tempDir.newFolder().toPath();

		byte[] content = ("<http://ex/s> <http://ex/p> \"o\"\n").getBytes(StandardCharsets.UTF_8);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (OutputStream out = new GZIPOutputStream(baos)) {
			out.write(content);
		}
		byte[] gzContent = baos.toByteArray();

		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.NT_SIMPLE_PARSER_KEY, "true", HDTOptionsKeys.LOADER_TYPE_KEY,
				HDTOptionsKeys.LOADER_TYPE_VALUE_DISK, HDTOptionsKeys.LOADER_DISK_LOCATION_KEY,
				root.resolve("work").toAbsolutePath().toString(), HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY,
				"2", HDTOptionsKeys.LOADER_DISK_CHUNK_SIZE_KEY, "1");

		try (InputStream in = new ByteArrayInputStream(gzContent);
				HDT hdt = HDTManager.generateHDT(in, "http://ex/", "data.nt.gz", spec, null)) {
			assertEquals(1, hdt.getTriples().getNumberOfElements());
		}
	}
}
