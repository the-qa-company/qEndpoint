package com.the_qa_company.qendpoint.core.hdt;

import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.impl.HDTDiskImporter;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.CompressTripleMapper;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class HDTDiskImporterBucketedMappingTest {
	private static final String BUCKETED_MAPPING_KEY = "loader.disk.bucketedMapping";

	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void createsBucketedMappingDirectoryWhenEnabled()
			throws IOException, ParserException, ReflectiveOperationException {
		Path root = tempDir.newFolder().toPath();
		Path workDir = root.resolve("work").toAbsolutePath();

		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, workDir.toString(),
				HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY, "1", HDTOptionsKeys.LOADER_DISK_CHUNK_SIZE_KEY, "1",
				BUCKETED_MAPPING_KEY, "true");

		List<TripleString> triples = List.of(new TripleString("<http://ex/s>", "<http://ex/p>", "\"o\""),
				new TripleString("<http://ex/s2>", "<http://ex/p2>", "<http://ex/o2>"));

		try (HDTDiskImporter importer = new HDTDiskImporter(spec, null, "http://ex/")) {
			CompressTripleMapper mapper = importer.compressDictionary(triples.iterator());
			mapper.delete();

			Field basePathField = HDTDiskImporter.class.getDeclaredField("basePath");
			basePathField.setAccessible(true);
			CloseSuppressPath basePath = (CloseSuppressPath) basePathField.get(importer);
			assertTrue(Files.isDirectory(basePath.resolve("bucketedMapping")));
		}
	}
}
