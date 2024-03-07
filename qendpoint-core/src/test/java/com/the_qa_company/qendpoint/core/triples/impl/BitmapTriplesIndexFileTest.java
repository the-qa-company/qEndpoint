package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.hdt.HDTVersion;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import org.apache.commons.io.file.PathUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.*;

public class BitmapTriplesIndexFileTest {

	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	public long crc32(byte[] data) {
		CRC32 crc = new CRC32();
		crc.update(data, 0, data.length);
		return crc.getValue();
	}

	@Test
	public void genTest() throws IOException, ParserException {
		Path root = tempDir.newFolder().toPath();

		HDTOptions spec = HDTOptions.of(
				HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS, "spo,ops",
				HDTOptionsKeys.BITMAPTRIPLES_INDEX_NO_FOQ, true
		);
		try {
			Path hdtPath = root.resolve("temp.hdt");

			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxTriples(1000, 10)
					.withMaxLiteralSize(50)
					.withMaxElementSplit(20);

			supplier.createAndSaveFakeHDT(spec, hdtPath);

			// should load
			HDTManager.mapIndexedHDT(hdtPath, spec, ProgressListener.ignore()).close();
			assertTrue("ops index doesn't exist", Files.exists(BitmapTriplesIndexFile.getIndexPath(hdtPath, TripleComponentOrder.OPS)));
			assertFalse("foq index exists", Files.exists(hdtPath.resolveSibling(hdtPath.getFileName() + HDTVersion.get_index_suffix("-"))));

			long crcold = crc32(Files.readAllBytes(hdtPath));

			Path hdtPath2 = root.resolve("temp2.hdt");

			Files.move(hdtPath, hdtPath2);

			supplier.createAndSaveFakeHDT(spec, hdtPath);
			// should erase the previous index and generate another one
			HDTManager.mapIndexedHDT(hdtPath, spec, ProgressListener.ignore()).close();

			long crcnew = crc32(Files.readAllBytes(hdtPath));

			assertNotEquals("files are the same", crcold, crcnew);
		} finally {
			PathUtils.deleteDirectory(root);
		}
	}
}