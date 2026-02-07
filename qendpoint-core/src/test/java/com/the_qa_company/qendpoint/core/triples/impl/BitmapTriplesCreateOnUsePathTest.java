package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.options.HDTSpecification;
import org.apache.commons.io.file.PathUtils;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BitmapTriplesCreateOnUsePathTest {
	@Test
	public void deletesTempDirectoryWhenClosed() throws Exception {
		HDTSpecification spec = new HDTSpecification();
		spec.set(HDTOptionsKeys.BITMAPTRIPLES_SEQUENCE_DISK, "true");

		Path path = null;
		try (BitmapTriples triples = new BitmapTriples(spec)) {
			BitmapTriples.CreateOnUsePath location = triples.getDiskSequenceLocation();
			assertNotNull(location);

			path = location.createOrGetPath();
			assertTrue("Expected temp directory to exist", Files.exists(path));
		}

		assertNotNull(path);
		boolean existsAfterClose = Files.exists(path);
		if (existsAfterClose) {
			PathUtils.deleteDirectory(path);
		}
		assertFalse("Expected temp directory to be deleted on close", existsAfterClose);
	}
}
