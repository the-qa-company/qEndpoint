package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.ControlInformation;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.options.HDTSpecification;
import com.the_qa_company.qendpoint.core.triples.Triples;
import org.apache.commons.io.file.PathUtils;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;
import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.compact.sequence.Sequence;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
import com.the_qa_company.qendpoint.core.util.io.AbstractMapMemoryTest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.Assert.*;

@RunWith(Suite.class)
@Suite.SuiteClasses({ BitmapTriplesTest.HandTest.class, BitmapTriplesTest.DynamicTest.class })
public class BitmapTriplesTest {

	public static HDT loadOrMapIndexed(Path path, HDTOptions spec, boolean map) throws IOException {
		String location = path.toAbsolutePath().toString();
		if (map) {
			return HDTManager.mapIndexedHDT(location, spec, null);
		} else {
			return HDTManager.loadIndexedHDT(location, null, spec);
		}
	}

	public static void assertEqualsSeq(String name, Sequence seqE, Sequence seqA) {
		assertEquals(name, seqE.getNumberOfElements(), seqA.getNumberOfElements());

		for (long i = 0; i < seqE.getNumberOfElements(); i++) {
			long e = seqE.get(i);
			long a = seqA.get(i);

			assertEquals(name + " | longs #" + i + "/" + seqE.getNumberOfElements() + " aren't the same", e, a);
		}
	}

	private static String closeValues(Bitmap excepted, Bitmap actual, long id) {
		long start = Math.max(0, id - 5);
		long endE = Math.min(excepted.getNumBits(), id + 5);
		long endA = Math.min(actual.getNumBits(), id + 5);

		StringBuilder buffer = new StringBuilder("\n");

		buffer.append("excepted: ");
		for (long i = start; i < endA; i++) {
			buffer.append(actual.access(i) ? '1' : '0');
		}
		for (long i = endA; i < endE; i++) {
			buffer.append('0');
		}
		buffer.append('\n');
		buffer.append("actual:   ");
		for (long i = start; i < endE; i++) {
			buffer.append(excepted.access(i) ? '1' : '0');
		}
		for (long i = endE; i < endA; i++) {
			buffer.append('0');
		}
		return buffer.toString();
	}

	public static void assertEqualsBM(String name, Bitmap seqE, Bitmap seqA) {
		long start = Math.min(seqE.getNumBits(), seqA.getNumBits());
		long end = Math.max(seqE.getNumBits(), seqA.getNumBits());

		for (long i = 0; i < start; i++) {
			boolean e = seqE.access(i);
			boolean a = seqA.access(i);

			if (e != a) {
				fail(name + " | bits #" + i + "/" + seqE.getNumBits() + " aren't the same"
						+ closeValues(seqE, seqA, i));
			}
		}
		if (start != end) {
			if (end == seqE.getNumBits()) {
				for (long i = start; i < end; i++) {
					assertFalse(name + " | bits #" + i + "/" + seqE.getNumBits() + " aren't the same", seqE.access(i));
				}
			} else {
				for (long i = start; i < end; i++) {
					assertFalse(name + " | bits #" + i + "/" + seqE.getNumBits() + " aren't the same", seqA.access(i));
				}
			}
		}
	}

	public static void assertBitmapTriplesEquals(BitmapTriples excepted, BitmapTriples actual) {
		// maps
		assertAdjEquals("AdjY", excepted.getBitmapY(), excepted.getSeqY(), actual.getBitmapY(), actual.getSeqY());
		assertAdjEquals("AdjZ", excepted.getBitmapZ(), excepted.getSeqZ(), actual.getBitmapZ(), actual.getSeqZ());
		// INDEX
		assertAdjEquals("AdjIndex", excepted.getBitmapIndex(), excepted.getIndexZ(), actual.getBitmapIndex(),
				actual.getIndexZ());
		assertEqualsSeq("predicateCount", excepted.getPredicateCount(), actual.getPredicateCount());
	}

	private static String closeAdj(Bitmap bitmapExcepted, Sequence seqExcepted, Bitmap bitmapActual, Sequence seqActual,
			long id) {
		long start = Math.max(0, id - 10);
		long end = Math.min(bitmapExcepted.getNumBits(), id + 10);

		StringBuilder buffer = new StringBuilder("\n");

		buffer.append("excepted:\n");
		for (long i = start; i < end; i++) {
			boolean b = bitmapExcepted.access(i);
			buffer.append((b ? '1' : '0')).append("     ");
		}
		buffer.append('\n');
		for (long i = start; i < end; i++) {
			long l = seqExcepted.get(i);
			buffer.append(String.format("%05d ", l));
		}
		buffer.append('\n');
		buffer.append("actual:\n");
		for (long i = start; i < end; i++) {
			boolean b = bitmapActual.access(i);
			buffer.append((b ? '1' : '0')).append("     ");
		}
		buffer.append('\n');
		for (long i = start; i < end; i++) {
			long l = seqActual.get(i);
			buffer.append(String.format("%05d ", l));
		}
		return buffer.toString();
	}

	public static void assertAdjEquals(String name, Bitmap bitmapExcepted, Sequence seqExcepted, Bitmap bitmapActual,
			Sequence seqActual) {
		assertEquals(bitmapExcepted.getNumBits(), bitmapActual.getNumBits());
		assertEquals(seqExcepted.getNumberOfElements(), seqActual.getNumberOfElements());
		assertEquals(bitmapActual.getNumBits(), seqExcepted.getNumberOfElements());

		for (long i = 0; i < seqExcepted.getNumberOfElements(); i++) {
			boolean a = bitmapActual.access(i);
			boolean e = bitmapExcepted.access(i);
			long al = seqActual.get(i);
			long el = seqExcepted.get(i);

			if (a != e) {
				fail(name + "\n" + "Expected :" + a + "\n" + "Actual   :" + e + "\n"
						+ closeAdj(bitmapExcepted, seqExcepted, bitmapActual, seqActual, i));
			}

			if (al != el) {
				fail(name + "\n" + "Expected :" + el + "\n" + "Actual   :" + al + "\n"
						+ closeAdj(bitmapExcepted, seqExcepted, bitmapActual, seqActual, i));

			}
		}

	}

	public static abstract class AbstractTest extends AbstractMapMemoryTest {
		@Rule
		public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();
	}

	@RunWith(Parameterized.class)
	public static class DynamicTest extends AbstractTest {
		@Parameterized.Parameters(name = "indexing: {0}, dict {1}")
		public static Collection<Object[]> params() {
			return Stream
					.of(HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_VALUE_DISK,
							HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_VALUE_OPTIMIZED)
					.flatMap(indexMethod -> Stream
							.of(HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_SECTION,
									HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_QUAD_SECTION)
							.map(dict -> new Object[] { indexMethod, dict }))
					.toList();
		}

		@Parameterized.Parameter
		public String indexMethod;
		@Parameterized.Parameter(1)
		public String dict;

		public void diskBitmapIndexTest(boolean map, boolean disk) throws IOException, ParserException {
			Path root = tempDir.newFolder().toPath();

			Path hdt1Path = root.resolve("hdt1.hdt");
			Path hdt2Path = root.resolve("hdt2.hdt");

			try {
				// create 1 one fake HDT
				long maxTriples = 10_000L;
				try (HDT hdt = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(maxTriples, 42)
						.createFakeHDT(new HDTSpecification())) {
					hdt.saveToHDT(hdt1Path.toAbsolutePath().toString(), null);
				}
				// copy this fake HDT to another file (faster than creating the
				// same twice)
				Files.copy(hdt1Path, hdt2Path);

				// optDisk = DISK, optDefault = OLD IMPLEMENTATION
				HDTOptions optDisk = HDTOptions.of(HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_KEY, indexMethod,
						HDTOptionsKeys.DICTIONARY_TYPE_KEY, dict);
				HDTOptions optDefault = HDTOptions.of(HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_KEY,
						HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_VALUE_LEGACY, HDTOptionsKeys.DICTIONARY_TYPE_KEY,
						dict);

				// set config
				if (disk) {
					optDisk.setOptions(HDTOptionsKeys.BITMAPTRIPLES_SEQUENCE_DISK, true,
							HDTOptionsKeys.BITMAPTRIPLES_SEQUENCE_DISK_SUBINDEX, true,
							HDTOptionsKeys.BITMAPTRIPLES_SEQUENCE_DISK_LOCATION,
							root.resolve("indexdir").toAbsolutePath());
				}

				try (ByteArrayOutputStream indexDisk = new ByteArrayOutputStream();
						ByteArrayOutputStream indexDefault = new ByteArrayOutputStream()) {

					// create the index for both HDTs
					try (HDT hdt = loadOrMapIndexed(hdt1Path, optDisk, map && disk)) {
						Triples triples = hdt.getTriples();

						assertTrue(triples instanceof BitmapTriples);

						BitmapTriples bitmapTriples = (BitmapTriples) triples;

						if (disk) {
							assertNotNull(bitmapTriples.diskSequenceLocation);
							assertTrue(bitmapTriples.diskSequence);
						}

						try (HDT hdt2 = loadOrMapIndexed(hdt2Path, optDefault, map)) {
							Triples triples2 = hdt2.getTriples();

							assertTrue(triples2 instanceof BitmapTriples);

							BitmapTriples bitmapTriples2 = (BitmapTriples) triples2;

							if (disk) {
								assertNull(bitmapTriples2.diskSequenceLocation);
								assertFalse(bitmapTriples2.diskSequence);
							}
							bitmapTriples2.saveIndex(indexDefault, new ControlInformation(), null);

							assertBitmapTriplesEquals(bitmapTriples2, bitmapTriples);
						}

						bitmapTriples.saveIndex(indexDisk, new ControlInformation(), null);
					}

					// read and compare indexes
					byte[] indexDiskArr = indexDisk.toByteArray();
					byte[] indexDefaultArr = indexDefault.toByteArray();

					assertArrayEquals("index not equals", indexDefaultArr, indexDiskArr);
				}
			} finally {
				PathUtils.deleteDirectory(root);
			}
		}

		@Test
		public void diskBitmapMapIndexedTest() throws IOException, ParserException {
			diskBitmapIndexTest(false, true);
		}

		@Test
		public void diskBitmapLoadIndexedTest() throws IOException, ParserException {
			diskBitmapIndexTest(false, true);
		}

		@Test
		public void memBitmapMapIndexedTest() throws IOException, ParserException {
			diskBitmapIndexTest(false, false);
		}

		@Test
		public void memBitmapLoadIndexedTest() throws IOException, ParserException {
			diskBitmapIndexTest(false, false);
		}
	}

	@Ignore("Hand tests")
	public static class HandTest extends AbstractTest {
		@Test
		public void readBadChannelTest() throws IOException {
			// java.nio.file.NoSuchFileException
			try (FileChannel channel = FileChannel.open(Path.of(UUID.randomUUID().toString()),
					StandardOpenOption.READ)) {
				System.out.println(channel.isOpen());
			}

		}

		@Test
		public void largeTest() throws IOException {
			/*
			 * Hand test to test the indexing of a large HDT with the disk
			 * indexer, hdtFile is the file to index, workDir is the location to
			 * handle the indexing.
			 */
			Path hdtFile = Path.of("N:\\qEndpoint\\qendpoint\\hdt-store\\index_dev.hdt"); // ASC42
			Path workdir = Path.of("N:\\WIP\\bitmaptriples");
			// Path hdtFile =
			// Path.of("C:\\Users\\wilat\\workspace\\qEndpoint\\qendpoint\\hdt-store\\index_dev.hdt");
			// //NAMAW
			// Path workDir = tempDir.newFolder().toPath();

			Files.createDirectories(workdir);

			HDTOptions opt = HDTOptions.of(HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_KEY,
					HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_VALUE_DISK, HDTOptionsKeys.BITMAPTRIPLES_SEQUENCE_DISK,
					true, HDTOptionsKeys.BITMAPTRIPLES_SEQUENCE_DISK_SUBINDEX, true,
					HDTOptionsKeys.BITMAPTRIPLES_SEQUENCE_DISK_LOCATION, workdir);

			try {
				HDTManager.mapIndexedHDT(hdtFile, opt, ProgressListener.sout()).close();
			} finally {
				if (Files.exists(workdir)) {
					PathUtils.deleteDirectory(workdir);
				}
			}
		}
	}
}
