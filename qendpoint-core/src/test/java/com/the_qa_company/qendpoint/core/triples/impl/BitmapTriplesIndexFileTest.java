package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.hdt.HDTManagerTest;
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
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

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

		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS, "spo,ops",
				HDTOptionsKeys.BITMAPTRIPLES_INDEX_NO_FOQ, true
		// , "debug.bitmaptriples.allowFastSort", true
		);
		try {
			Path hdtPath = root.resolve("temp.hdt");

			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxTriples(1000, 10).withMaxLiteralSize(50).withMaxElementSplit(20);

			supplier.createAndSaveFakeHDT(spec, hdtPath);

			// should load
			HDTManager.mapIndexedHDT(hdtPath, spec, ProgressListener.ignore()).close();
			assertTrue("ops index doesn't exist",
					Files.exists(BitmapTriplesIndexFile.getIndexPath(hdtPath, TripleComponentOrder.OPS)));
			assertFalse("foq index exists",
					Files.exists(hdtPath.resolveSibling(hdtPath.getFileName() + HDTVersion.get_index_suffix("-"))));

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

	private void assertBitmapTriplesIndexFileEquals(Path hdtPath, Path expected, Path actual) throws IOException {
		try (HDT hdt = HDTManager.mapHDT(hdtPath);
				FileChannel exch = FileChannel.open(expected, StandardOpenOption.READ);
				FileChannel acch = FileChannel.open(actual, StandardOpenOption.READ);) {
			BitmapTriplesIndex ex = BitmapTriplesIndexFile.map(expected, exch, (BitmapTriples) hdt.getTriples(), false);
			BitmapTriplesIndex ac = BitmapTriplesIndexFile.map(actual, acch, (BitmapTriples) hdt.getTriples(), false);

			assertEquals(ex.getOrder(), ac.getOrder());

			long ny = ac.getAdjacencyListY().getNumberOfElements();
			long nz = ac.getAdjacencyListZ().getNumberOfElements();

			assertEquals(ex.getAdjacencyListY().getNumberOfElements(), ny);
			assertEquals(ex.getAdjacencyListZ().getNumberOfElements(), nz);

			for (long i = 0; i < ny; i++) {
				assertEquals("invalid adjy #" + i, ex.getAdjacencyListY().get(i), ac.getAdjacencyListY().get(i));
				assertEquals("invalid adjy #" + i, ex.getBitmapY().access(i), ac.getBitmapY().access(i));
			}
			for (long i = 0; i < nz; i++) {
				assertEquals("invalid adjz #" + i, ex.getAdjacencyListZ().get(i), ac.getAdjacencyListZ().get(i));
				assertEquals("invalid adjz #" + i, ex.getBitmapZ().access(i), ac.getBitmapZ().access(i));
			}
		}

	}

	@Test
	public void genFastSortTest() throws IOException, ParserException, NotFoundException {
		Path root = tempDir.newFolder().toPath();

		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS, "spo,sop,ops,osp,pos,pso",
				HDTOptionsKeys.BITMAPTRIPLES_INDEX_NO_FOQ, true);
		try {
			Path hdtPath = root.resolve("temp.hdt");
			Path hdtPath2 = root.resolve("temp2.hdt");

			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxTriples(10000, 10).withMaxLiteralSize(50).withMaxElementSplit(20);

			supplier.createAndSaveFakeHDT(spec, hdtPath);

			// should load
			HDTManager.mapIndexedHDT(hdtPath, spec, ProgressListener.ignore()).close();
			Path ospPath = BitmapTriplesIndexFile.getIndexPath(hdtPath, TripleComponentOrder.OSP);
			Path opsPath = BitmapTriplesIndexFile.getIndexPath(hdtPath, TripleComponentOrder.OPS);
			Path posPath = BitmapTriplesIndexFile.getIndexPath(hdtPath, TripleComponentOrder.POS);
			Path psoPath = BitmapTriplesIndexFile.getIndexPath(hdtPath, TripleComponentOrder.PSO);
			Path sopPath = BitmapTriplesIndexFile.getIndexPath(hdtPath, TripleComponentOrder.SOP);
			assertTrue("osp index doesn't exist", Files.exists(ospPath));
			assertTrue("ops index doesn't exist", Files.exists(opsPath));
			assertFalse("foq index exists",
					Files.exists(hdtPath.resolveSibling(hdtPath.getFileName() + HDTVersion.get_index_suffix("-"))));

			Path ospPathOld = ospPath.resolveSibling(ospPath.getFileName() + "2");
			Path opsPathOld = opsPath.resolveSibling(opsPath.getFileName() + "2");
			Path posPathOld = posPath.resolveSibling(posPath.getFileName() + "2");
			Path psoPathOld = psoPath.resolveSibling(psoPath.getFileName() + "2");
			Path sopPathOld = sopPath.resolveSibling(sopPath.getFileName() + "2");

			Files.move(ospPath, ospPathOld);
			Files.move(opsPath, opsPathOld);
			Files.move(posPath, posPathOld);
			Files.move(psoPath, psoPathOld);
			Files.move(sopPath, sopPathOld);

			Files.move(hdtPath, hdtPath2);

			spec.set("debug.bitmaptriples.allowFastSort", false);
			supplier.reset();
			supplier.createAndSaveFakeHDT(spec, hdtPath);
			// should erase the previous index and generate another one
			HDTManager.mapIndexedHDT(hdtPath, spec, ProgressListener.ignore()).close();

			try (HDT hdt1 = HDTManager.mapHDT(hdtPath); HDT hdt2 = HDTManager.mapHDT(hdtPath2)) {
				HDTManagerTest.HDTManagerTestBase.assertEqualsHDT(hdt1, hdt2);
			}

			assertBitmapTriplesIndexFileEquals(hdtPath, sopPath, sopPathOld);
			assertBitmapTriplesIndexFileEquals(hdtPath, posPath, posPathOld);
			assertBitmapTriplesIndexFileEquals(hdtPath, psoPath, psoPathOld);
			assertBitmapTriplesIndexFileEquals(hdtPath, ospPath, ospPathOld);
			assertBitmapTriplesIndexFileEquals(hdtPath, opsPath, opsPathOld);

		} finally {
			PathUtils.deleteDirectory(root);
		}
	}
}
