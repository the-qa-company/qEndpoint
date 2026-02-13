package com.the_qa_company.qendpoint.core.iterator.utils;

import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.impl.HDTDiskImporter;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.CompressTripleMapper;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class IteratorChunkedSourceUsageTest {
	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	@Test
	public void nonSimpleParserDoesNotUseChunkedSource() throws IOException, ParserException {
		int originalBufferSize = IteratorChunkedSource.bufferSize;
		int observedBufferSize;
		HDTDiskImporter importer = null;
		CompressTripleMapper mapper = null;
		try {
			IteratorChunkedSource.bufferSize = 4;

			Path root = tempDir.newFolder().toPath();
			List<TripleString> triples = new ArrayList<>();
			for (int i = 0; i < 10; i++) {
				triples.add(new TripleString("http://ex/s" + i, "http://ex/p", "o"));
			}

			HDTOptions spec = HDTOptions.of(HDTOptionsKeys.NT_SIMPLE_PARSER_KEY, "false",
					HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, root.resolve("work").toAbsolutePath().toString(),
					HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY, "2", HDTOptionsKeys.LOADER_DISK_CHUNK_SIZE_KEY,
					"1048576");

			importer = new HDTDiskImporter(spec, null, "http://ex/");
			mapper = importer.compressDictionary(triples.iterator());

			observedBufferSize = IteratorChunkedSource.bufferSize;
		} finally {
			if (mapper != null) {
				mapper.delete();
			}
			if (importer != null) {
				importer.close();
			}
			IteratorChunkedSource.bufferSize = originalBufferSize;
		}

		assertEquals(4, observedBufferSize);
	}
}
