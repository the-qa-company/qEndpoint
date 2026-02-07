package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.options.HDTSpecification;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
import org.apache.commons.io.file.PathUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class BitmapTriplesObjectIndexProgressTest {
	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	int before;

	@Before
	public void setUp() {
		before = BitmapTriples.REPORT_INTERVAL;
		BitmapTriples.REPORT_INTERVAL = 1;
	}

	@After
	public void tearDown() {
		BitmapTriples.REPORT_INTERVAL = before;
	}

	@Test
	public void objectIndexProgressReportsStagesAndThroughput() throws Exception {
		Path root = tempDir.newFolder().toPath();
		Path hdtPath = root.resolve("hdt.hdt");

		try {
			try (HDT hdt = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(500_000L, 42)
					.createFakeHDT(new HDTSpecification())) {
				hdt.saveToHDT(hdtPath.toAbsolutePath().toString(), null);
			}

			List<String> messages = new ArrayList<>();
			ProgressListener listener = (level, message) -> messages.add(message);

			HDTOptions options = HDTOptions.of(HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_KEY,
					HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_VALUE_OPTIMIZED);

			try (HDT indexed = HDTManager.loadIndexedHDT(hdtPath, listener, options)) {
				assertTrue(indexed.getTriples() instanceof BitmapTriples);
			}

			assertTrue("stage messages present", messages.stream().anyMatch(m -> m.contains("object index:")));
			assertTrue("count objects stage present",
					messages.stream().anyMatch(m -> m.contains("object index: count objects")));
			assertTrue("materialize object counts stage present",
					messages.stream().anyMatch(m -> m.contains("object index: materialize object counts")));
			long materializeCountMessages = messages.stream()
					.filter(m -> m.contains("object index: materialize object counts")).count();
			assertTrue("materialize object counts reports progress updates", materializeCountMessages > 2);
			assertTrue("thousands separator present",
					messages.stream().anyMatch(m -> m.matches(".*\\d{1,3},\\d{3}.*")));
			assertTrue("throughput present", messages.stream().anyMatch(m -> m.contains("items/s")));
		} finally {
			PathUtils.deleteDirectory(root);
		}
	}
}
