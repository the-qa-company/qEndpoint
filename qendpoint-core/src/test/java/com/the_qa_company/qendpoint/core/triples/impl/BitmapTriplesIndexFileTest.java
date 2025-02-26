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
import com.the_qa_company.qendpoint.core.util.StopWatch;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.io.AbstractMapMemoryTest;
import org.apache.commons.io.file.PathUtils;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.Assert.*;

public class BitmapTriplesIndexFileTest extends AbstractMapMemoryTest {

	public static final long COUNT = 1000;

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
				HDTOptionsKeys.BITMAPTRIPLES_INDEX_NO_FOQ, true,
				"debug.bitmaptriples.fastSortCheckSubjectGroups", true
		// , "debug.bitmaptriples.allowFastSort", true
		);
		Path hdtPath = root.resolve("temp.hdt");

		LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(COUNT, 10)
				.withMaxLiteralSize(50).withMaxElementSplit(20);

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
		PathUtils.deleteDirectory(root);
	}

	private void assertBitmapTriplesIndexFileEquals(Path hdtPath, Path expected, Path actual) throws IOException {
		try (HDT hdt = HDTManager.mapHDT(hdtPath);
				FileChannel exch = FileChannel.open(expected, StandardOpenOption.READ);
				FileChannel acch = FileChannel.open(actual, StandardOpenOption.READ);
				BitmapTriplesIndex ex = BitmapTriplesIndexFile.map(expected, exch, (BitmapTriples) hdt.getTriples(),
						false);
				BitmapTriplesIndex ac = BitmapTriplesIndexFile.map(actual, acch, (BitmapTriples) hdt.getTriples(),
						false)) {

			assertEquals(ex.getOrder(), ac.getOrder());

			long ny = ac.getAdjacencyListY().getNumberOfElements();
			long nz = ac.getAdjacencyListZ().getNumberOfElements();

			assertEquals("Invalid AdjacencyListY sizes", ex.getAdjacencyListY().getNumberOfElements(), ny);
			assertEquals("Invalid AdjacencyListZ sizes", ex.getAdjacencyListZ().getNumberOfElements(), nz);

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
	public void genNoFastSortTest() throws IOException, ParserException {
		Path root = tempDir.newFolder().toPath();

		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS, "spo,sop,ops,osp,pos,pso",
				HDTOptionsKeys.BITMAPTRIPLES_INDEX_NO_FOQ, true, "debug.bitmaptriples.allowFastSort", false
				, "debug.bitmaptriples.fastSortCheckSubjectGroups", true);
		Path hdtPath = root.resolve("temp.hdt");

		LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(COUNT, 10)
				.withMaxLiteralSize(50).withMaxElementSplit(20);

		supplier.createAndSaveFakeHDT(spec, hdtPath);

		// should load
		HDTManager.mapIndexedHDT(hdtPath, spec, ProgressListener.ignore()).close();
		PathUtils.deleteDirectory(root);
	}


	@Test
	public void genFastSortTest() throws IOException, ParserException, NotFoundException {
		Path root = tempDir.newFolder().toPath();

		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS, "spo,sop,ops,osp,pos,pso",
				HDTOptionsKeys.BITMAPTRIPLES_INDEX_NO_FOQ, true, "debug.bitmaptriples.allowFastSortV2", false
				, "debug.bitmaptriples.fastSortCheckSubjectGroups", true);
		Path hdtPath = root.resolve("temp.hdt");
		Path hdtPath2 = root.resolve("temp2.hdt");

		LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(COUNT, 10)
				.withMaxLiteralSize(50).withMaxElementSplit(20);

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

		PathUtils.deleteDirectory(root);
	}

	@Test
	public void genFastSortV2Test() throws IOException, ParserException, NotFoundException {
		Path root = tempDir.newFolder().toPath();

		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS, "spo,sop,ops,osp,pos,pso",
				HDTOptionsKeys.BITMAPTRIPLES_INDEX_NO_FOQ, true, "debug.bitmaptriples.fastSortCheckSubjectGroups", true);
		Path hdtPath = root.resolve("temp.hdt");
		Path hdtPath2 = root.resolve("temp2.hdt");

		LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(COUNT, 10)
				.withMaxLiteralSize(50).withMaxElementSplit(20);

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

		spec.set("debug.bitmaptriples.allowFastSortV2", false);
		supplier.reset();
		supplier.createAndSaveFakeHDT(spec, hdtPath);
		// should erase the previous index and generate another one
		HDTManager.mapIndexedHDT(hdtPath, spec, ProgressListener.ignore()).close();

		try (HDT hdt1 = HDTManager.mapHDT(hdtPath); HDT hdt2 = HDTManager.mapHDT(hdtPath2)) {
			HDTManagerTest.HDTManagerTestBase.assertEqualsHDT(hdt1, hdt2);
		}

		assertBitmapTriplesIndexFileEquals(hdtPath, sopPath, sopPathOld);
		assertBitmapTriplesIndexFileEquals(hdtPath, ospPath, ospPathOld);
		assertBitmapTriplesIndexFileEquals(hdtPath, opsPath, opsPathOld);
		assertBitmapTriplesIndexFileEquals(hdtPath, posPath, posPathOld);
		assertBitmapTriplesIndexFileEquals(hdtPath, psoPath, psoPathOld);

		PathUtils.deleteDirectory(root);
	}


	@Test
	@Ignore("hand")
	public void genHandTest() throws IOException, ParserException {
		Path root = tempDir.newFolder().toPath();

		HDTOptions spec = HDTOptions.of(
				HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS, "spo,sop,ops,osp,pos,pso",
				"debug.bitmaptriples.allowFastSortV2", true,
				"debug.bitmaptriples.allowFastSort", true,
				HDTOptionsKeys.BITMAPTRIPLES_INDEX_NO_FOQ, true
		);

		LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(10000000, 10)
				.withMaxLiteralSize(20).withMaxElementSplit(50);

		Path hdtPath = root.resolve("test.hdt");

		supplier.createAndSaveFakeHDT(spec, hdtPath);

		// should load
		StopWatch sw = new StopWatch();
		HDTManager.mapIndexedHDT(hdtPath, spec, ProgressListener.ignore()).close();
		System.out.println("done in " + sw.stopAndShow());
		// v time 1000000
		// 0 6 sec 145 ms
		// 1 4 sec 200 ms
		// 2 4 sec


		// 50/20
		// v time 10000000
		// 0 41 sec 903 ms 915 us
		/*
17:22:41.544 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx SPO->SOP
17:22:50.009 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx SPO->SOP in 8 sec 464 ms 738 us
17:22:50.106 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx SPO->PSO
17:22:56.834 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx SPO->PSO in 6 sec 727 ms 138 us
17:22:56.873 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx PSO->POS
17:23:04.012 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx PSO->POS in 7 sec 139 ms 476 us
17:23:04.042 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx SPO->OSP
17:23:11.028 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx SPO->OSP in 6 sec 985 ms 436 us
17:23:11.052 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx OSP->OPS
17:23:23.212 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx OSP->OPS in 12 sec 159 ms 983 us

		 */
		// 1 24 sec 156 ms 929 us
		/*

17:20:57.485 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx SPO->SOP in 4 sec 279 ms 96 us
17:20:57.564 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx SPO->PSO
17:21:04.552 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx SPO->PSO in 6 sec 987 ms 580 us
17:21:04.587 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx PSO->POS
17:21:07.428 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx PSO->POS in 2 sec 841 ms 380 us
17:21:07.458 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx SPO->OSP
17:21:15.184 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx SPO->OSP in 7 sec 726 ms 327 us
17:21:15.217 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx OSP->OPS
17:21:17.012 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx OSP->OPS in 1 sec 795 ms 943 us

		 */
		// 2 23 sec 816 ms 314 us
		/*
17:24:36.733 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx SPO->SOP
17:24:39.549 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx SPO->SOP in 2 sec 816 ms 169 us
17:24:39.654 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx SPO->PSO
17:24:47.352 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx SPO->PSO in 7 sec 698 ms 36 us
17:24:47.391 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx PSO->POS
17:24:49.945 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx PSO->POS in 2 sec 553 ms 610 us
17:24:49.973 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx SPO->OSP
17:24:58.481 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx SPO->OSP in 8 sec 507 ms 40 us
17:24:58.506 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx OSP->OPS
17:25:00.258 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx OSP->OPS in 1 sec 752 ms 118 us
		 */
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		// 20/50
		// v time 10000000
		// 0 42 sec 84 ms 290 us
		/*
17:46:05.225 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx SPO->SOP
17:46:14.023 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx SPO->SOP in 8 sec 798 ms 285 us
17:46:14.089 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx SPO->PSO
17:46:22.684 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx SPO->PSO in 8 sec 594 ms 561 us
17:46:22.717 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx PSO->POS
17:46:30.713 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx PSO->POS in 7 sec 995 ms 910 us
17:46:30.744 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx SPO->OSP
17:46:38.739 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx SPO->OSP in 7 sec 995 ms 75 us
17:46:38.777 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx OSP->OPS
17:46:47.085 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx OSP->OPS in 8 sec 309 ms 100 us
		 */
		// 1
		/*
17:43:13.434 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx SPO->SOP
17:43:17.835 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx SPO->SOP in 4 sec 402 ms 196 us
17:43:17.898 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx SPO->PSO
17:43:26.837 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx SPO->PSO in 8 sec 939 ms 3 us
17:43:26.878 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx PSO->POS
17:43:30.978 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx PSO->POS in 4 sec 100 ms 326 us
17:43:31.020 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx SPO->OSP
17:43:40.733 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx SPO->OSP in 9 sec 712 ms 534 us
17:43:40.776 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx OSP->OPS
17:43:43.240 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx OSP->OPS in 2 sec 463 ms 484 us
		 */
		// 2 32 sec 586 ms 978 us
		/*
17:41:26.768 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx SPO->SOP
17:41:31.567 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx SPO->SOP in 4 sec 799 ms 37 us
17:41:31.688 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx SPO->PSO
17:41:42.630 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx SPO->PSO in 10 sec 941 ms 223 us
17:41:42.681 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx PSO->POS
17:41:46.415 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx PSO->POS in 3 sec 733 ms 525 us
17:41:46.454 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx SPO->OSP
17:41:56.367 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx SPO->OSP in 9 sec 913 ms 366 us
17:41:56.395 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - generate other idx OSP->OPS
17:41:58.867 [main] DEBUG c.t.q.c.triples.impl.BitmapTriples - end generate other idx OSP->OPS in 2 sec 472 ms 101 us
		 */
		PathUtils.deleteDirectory(root);
	}

}
