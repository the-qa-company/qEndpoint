package com.the_qa_company.qendpoint.core.hdt;

import com.the_qa_company.qendpoint.core.compact.bitmap.BitmapFactory;
import com.the_qa_company.qendpoint.core.compact.bitmap.ModifiableBitmap;
import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.dictionary.DictionaryFactory;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.impl.BaseDictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.MultipleBaseDictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.MultipleLangBaseDictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.MultipleSectionDictionaryLang;
import com.the_qa_company.qendpoint.core.dictionary.impl.MultipleSectionDictionaryLangPrefixes;
import com.the_qa_company.qendpoint.core.enums.CompressionType;
import com.the_qa_company.qendpoint.core.enums.RDFNodeType;
import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.CompressionResult;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.MapOnCallHDT;
import com.the_qa_company.qendpoint.core.iterator.utils.PipedCopyIterator;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.options.HDTSpecification;
import com.the_qa_company.qendpoint.core.rdf.RDFFluxStop;
import com.the_qa_company.qendpoint.core.rdf.RDFParserFactory;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriplesIteratorPositionTest;
import com.the_qa_company.qendpoint.core.triples.impl.utils.HDTTestUtils;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import com.the_qa_company.qendpoint.core.util.io.AbstractMapMemoryTest;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.io.compress.CompressTest;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.CharSequenceComparator;
import com.the_qa_company.qendpoint.core.util.string.PrefixesStorage;
import com.the_qa_company.qendpoint.core.util.string.ReplazableString;
import org.apache.commons.io.file.PathUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import static com.the_qa_company.qendpoint.core.enums.TripleComponentRole.OBJECT;
import static com.the_qa_company.qendpoint.core.enums.TripleComponentRole.SUBJECT;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Suite.class)
@Suite.SuiteClasses({ HDTManagerTest.DynamicDiskTest.class, HDTManagerTest.DynamicCatTreeTest.class,
		HDTManagerTest.FileDynamicTest.class, HDTManagerTest.StaticTest.class, HDTManagerTest.MSDLangTest.class,
		HDTManagerTest.HDTQTest.class, HDTManagerTest.DictionaryLangTypeTest.class,
		HDTManagerTest.MSDLangQuadTest.class })
public class HDTManagerTest {
	public static class HDTManagerTestBase extends AbstractMapMemoryTest implements ProgressListener {
		protected final Logger logger;

		protected static List<String> diskDict() {
			return List.of(HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_QUAD_SECTION,
					HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG_QUAD,
					HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS,
					HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_SECTION,
					HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG);
		}

		protected static List<String> diskDictCat() {
			return diskDict();
		}

		/**
		 * disable string order consistency test
		 * <a href="https://github.com/rdfhdt/hdt-java/issues/177">GH#177</a>
		 */
		protected static final boolean ALLOW_STRING_CONSISTENCY_TEST = false;
		protected static final long SIZE_VALUE = 1L << 10;
		protected static final int SEED = 67;

		private HDTManagerTestBase() {
			logger = LoggerFactory.getLogger(getClass());
		}

		@Rule
		public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();
		protected HDTSpecification spec;
		protected Path rootFolder;

		@Before
		public void setupManager() throws IOException {
			spec = new HDTSpecification();
			rootFolder = tempDir.newFolder().toPath();
			spec.set(HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, rootFolder.toAbsolutePath().toString());
		}

		@After
		public void closeManager() throws IOException {
			if (Files.exists(rootFolder)) {
				try (Stream<Path> s = Files.list(rootFolder)) {
					// might be wrong with some OS hidden files?
					assertFalse("root folder not empty", s.findAny().isPresent());
				}
			}
		}

		@Override
		public void notifyProgress(float level, String message) {
			// System.out.println("[" + level + "] " + message);
		}

		public static HDT combineHDTResult(HDTResult result, Path root) throws IOException {
			Path tempDir = root.resolve("tempdir");
			Path work = tempDir.resolve("work");

			HDTOptions spec = HDTOptions.of(HDTOptionsKeys.HDTCAT_LOCATION, work);

			Path results = tempDir.resolve("hdts-res");

			Files.createDirectories(results);

			List<Path> paths = new ArrayList<>();
			for (HDT hdt : result.getHdts()) {
				Path name = IOUtil.getUniqueNamePath(results, "hdtresult", ".hdt");
				hdt.saveToHDT(name);
				paths.add(name);
			}

			HDT hdtret = HDTManager.catHDTPath(paths, spec, ProgressListener.ignore());
			try {
				PathUtils.deleteDirectory(tempDir);
			} catch (AccessDeniedException e) {
				e.printStackTrace(); // wtf?
			} catch (Throwable e) {
				try {
					hdtret.close();
				} catch (Throwable t) {
					e.addSuppressed(t);
				}
				throw e;
			}
			return hdtret;
		}

		public static void assertIteratorEquals(Iterator<? extends CharSequence> it1,
				Iterator<? extends CharSequence> it2) {
			while (it1.hasNext()) {
				Assert.assertTrue(it2.hasNext());
				Assert.assertEquals(it1.next().toString(), it2.next().toString());
			}
			Assert.assertFalse(it2.hasNext());
		}

		public static void assertEqualsHDT(HDT expected, HDT actual) throws NotFoundException {
			assertEquals("non matching sizes", expected.getTriples().getNumberOfElements(),
					actual.getTriples().getNumberOfElements());
			// test dictionary
			Dictionary ed = expected.getDictionary();
			Dictionary ad = actual.getDictionary();
			assertEqualsHDT("Subjects", ed.getSubjects(), ad.getSubjects());
			assertEqualsHDT("Predicates", ed.getPredicates(), ad.getPredicates());

			PrefixesStorage eps = ed.getPrefixesStorage(false);
			PrefixesStorage aps = ad.getPrefixesStorage(false);

			assertEquals("prefixes storages aren't the same", eps, aps);

			if (ed instanceof MultipleBaseDictionary || ed instanceof MultipleSectionDictionaryLang|| ed instanceof MultipleSectionDictionaryLangPrefixes) {
				if (ed instanceof MultipleBaseDictionary) {
					assertTrue("ad not a MSD" + ad.getClass(), ad instanceof MultipleBaseDictionary);
				} else if (ed instanceof MultipleSectionDictionaryLang) {
					assertTrue("ad not a MSDL" + ad.getClass(), ad instanceof MultipleSectionDictionaryLang);
				} else {
					assertTrue("ad not a MSDLP" + ad.getClass(), ad instanceof MultipleSectionDictionaryLangPrefixes);
				}
				Map<? extends CharSequence, DictionarySection> keysE = ed.getAllObjects();
				Map<? extends CharSequence, DictionarySection> keysA = ad.getAllObjects();
				assertEquals(keysE.keySet(), keysA.keySet());
				Iterator<? extends CharSequence> itkE = keysE.keySet().iterator();
				Iterator<? extends CharSequence> itkA = keysA.keySet().iterator();

				// test nodes order
				assertIteratorEquals(itkE, itkA);

				keysE.forEach((key, dictE) -> {
					DictionarySection dictA = keysA.get(key);

					assertEqualsHDT(key.toString(), dictE, dictA);
				});
			} else {
				assertFalse("actual dictionary is of type MultipleBaseDictionary, but ed is, actual: " + ad.getClass()
						+ ", excepted: " + ed.getClass(), ad instanceof MultipleBaseDictionary);
				assertEqualsHDT("Objects", ed.getObjects(), ad.getObjects());
			}
			assertEqualsHDT("Shared", ed.getShared(), ad.getShared());
			assertEquals(ed.getType(), ad.getType());

			// test triples
			IteratorTripleID actualIt = actual.getTriples().searchAll();
			IteratorTripleID expectedIt = expected.getTriples().searchAll();

			while (expectedIt.hasNext()) {
				assertTrue(actualIt.hasNext());

				TripleID expectedTriple = expectedIt.next();
				TripleID actualTriple = actualIt.next();

				long location = expectedIt.getLastTriplePosition();
				assertEquals("The tripleID location doesn't match", location, actualIt.getLastTriplePosition());
				assertEquals("The tripleID #" + location + " doesn't match", expectedTriple, actualTriple);
			}
			assertFalse(actualIt.hasNext());

			// test header
			assertEquals(actual.getHeader().getBaseURI(), expected.getHeader().getBaseURI());
			if (expected.getHeader().getNumberOfElements() != actual.getHeader().getNumberOfElements()) {
				StringBuilder bld = new StringBuilder();

				bld.append("-------- Header excepted:");
				expected.getHeader().search("", "", "").forEachRemaining(l -> bld.append(l).append('\n'));
				bld.append("-------- Header actual:");
				actual.getHeader().search("", "", "").forEachRemaining(l -> bld.append(l).append('\n'));

				fail("Size of the header doesn't match " + bld + expected.getHeader().getNumberOfElements() + "!="
						+ actual.getHeader().getNumberOfElements());
			}
		}

		public static void checkHDTConsistency(HDT hdt) {
			Dictionary dict = hdt.getDictionary();
			Map<CharSequence, DictionarySection> map;
			map = new HashMap<>();
			if (dict instanceof MultipleBaseDictionary || dict instanceof MultipleLangBaseDictionary) {
				map.putAll(dict.getAllObjects());
			} else {
				map.put("Objects", dict.getObjects());
			}
			map.put("Subjects", dict.getSubjects());
			map.put("Predicates", dict.getPredicates());
			map.put("Shared", dict.getShared());

			if (dict.supportGraphs()) {
				map.put("Graph", dict.getGraphs());
			}

			ReplazableString prev = new ReplazableString();
			Comparator<CharSequence> cmp = CharSequenceComparator.getInstance();
			map.forEach((name, section) -> {
				prev.clear();
				String prev2 = "";
				Iterator<? extends CharSequence> it = section.getSortedEntries();
				if (it.hasNext()) {
					prev.replace(it.next());
				}

				while (it.hasNext()) {
					CharSequence next = ByteString.of(it.next());

					int cmpV = cmp.compare(prev, next);
					if (cmpV >= 0) {
						System.out.print("Prev: ");
						printHex(prev);
						System.out.print("Next: ");
						printHex(next);
						System.out.print("Prev: ");
						printBin(prev);
						System.out.print("Next: ");
						printBin(next);

						if (cmpV == 0) {
							fail("[" + name + "] (BS) Duplicated elements! " + prev + " = " + next);
						}
						fail("[" + name + "] (BS) Bad order! " + prev + " > " + next);
					}

					if (ALLOW_STRING_CONSISTENCY_TEST) {
						String nextStr = next.toString();
						int cmpV2 = cmp.compare(prev2, nextStr);
						if (cmpV2 == 0) {
							fail("[" + name + "] (Str) Duplicated elements! " + prev2 + " = " + next);
						}
						if (cmpV2 > 0) {
							fail("[" + name + "] (Str) Bad order! " + prev2 + " > " + next);
						}

						assertEquals("str and byteStr compare aren't returning the same results", Math.signum(cmpV2),
								Math.signum(cmpV), 0.01);
						prev2 = nextStr;
					}
					prev.replace(next);
				}
			});
			IteratorTripleID tripleIt = hdt.getTriples().searchAll();
			long count = 0;
			TripleID last = new TripleID(-1, -1, -1);
			while (tripleIt.hasNext()) {
				TripleID tid = tripleIt.next();
				if (tid.match(last)) { // same graph?
					continue;
				}
				count++;
				last.setAll(tid.getSubject(), tid.getPredicate(), tid.getObject());
			}
			assertEquals("tripleIt:" + tripleIt.getClass(), hdt.getTriples().getNumberOfElements(), count);
		}

		public static void assertComponentsNotNull(String message, TripleString ts) {
			if (ts.getSubject() == null || ts.getSubject().isEmpty() || ts.getPredicate() == null
					|| ts.getPredicate().isEmpty() || ts.getObject() == null || ts.getObject().isEmpty()) {
				fail(message + " (" + ts + ")");
			}
		}

		public static void assertEqualsHDT(String section, DictionarySection excepted, DictionarySection actual) {
			assertEquals("sizes of section " + section + " aren't the same!", excepted.getNumberOfElements(),
					actual.getNumberOfElements());
			Iterator<? extends CharSequence> itEx = excepted.getSortedEntries();
			Iterator<? extends CharSequence> itAc = actual.getSortedEntries();

			while (itEx.hasNext()) {
				assertTrue("dictionary section " + section + " is less big than excepted", itAc.hasNext());
				CharSequence expectedTriple = itEx.next();
				CharSequence actualTriple = itAc.next();
				CompressTest.assertCharSequenceEquals(section + " section strings", expectedTriple, actualTriple);
			}
			assertFalse("dictionary section " + section + " is bigger than excepted", itAc.hasNext());
		}

		protected static void printHex(CharSequence seq) {
			ByteString bs = ByteString.of(seq);
			byte[] buffer = bs.getBuffer();
			int len = bs.length();
			for (int i = 0; i < len; i++) {
				System.out.printf("%2x ", buffer[i] & 0xFF);
			}
			System.out.println();
		}

		protected static void printBin(CharSequence seq) {
			ByteString bs = ByteString.of(seq);
			byte[] buffer = bs.getBuffer();
			int len = bs.length();
			for (int i = 0; i < len; i++) {
				System.out.print(Integer.toBinaryString(buffer[i] & 0xFF) + " ");
			}
			System.out.println();
		}
	}

	@RunWith(Parameterized.class)
	public static class DynamicDiskTest extends HDTManagerTestBase {

		@Parameterized.Parameters(name = "{7} - {0} - {10}")
		public static Collection<Object[]> params() {
			List<Object[]> params = new ArrayList<>();
			for (String dict : diskDict()) {
				params.addAll(List.of(
						new Object[] { "slow-str1", 10, 2, 4, 2,
								HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_VALUE_COMPLETE, false, dict, 2,
								"debug.disk.slow.stream=true", "" },
						new Object[] { "slow-str2", 10, 2, 4, 2,
								HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_VALUE_COMPLETE, false, dict, 2,
								"debug.disk.slow.stream2=true", "" },
						new Object[] { "slow-cfsd", 10, 2, 4, 2,
								HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_VALUE_COMPLETE, false, dict, 2,
								"debug.disk.slow.pfsd=true", "" },
						new Object[] { "slow-kw-d", 10, 2, 4, 2,
								HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_VALUE_COMPLETE, false, dict, 2,
								"debug.disk.slow.kway.dict=true", "" },
						new Object[] { "slow-kw-t", 10, 2, 4, 2,
								HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_VALUE_COMPLETE, false, dict, 2,
								"debug.disk.slow.kway.triple=true", "" }));
				for (int threads : new int[] {
						// sync
						1,
						// async, low thread count
						2,
						// async, large thread count
						8 }) {
					// HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_VALUE_PARTIAL,
					List<String> modes = List.of(HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_VALUE_COMPLETE);
					for (String mode : modes) {
						params.addAll(List.of(
								new Object[] { "base-w" + threads + "-" + mode, SIZE_VALUE * 8, 20, 50, threads, mode,
										false, dict, SIZE_VALUE, "", "" },
								new Object[] { "duplicates-w" + threads + "-" + mode, SIZE_VALUE * 8, 10, 50, threads,
										mode, false, dict, SIZE_VALUE, "", "" },
								new Object[] { "large-literals-w" + threads + "-" + mode, SIZE_VALUE * 2, 20, 250,
										threads, mode, false, dict, SIZE_VALUE, "", "" },
								new Object[] { "quiet-w" + threads + "-" + mode, SIZE_VALUE * 8, 10, 50, threads, mode,
										false, dict, SIZE_VALUE, "", "" }));
					}
				}
			}

			for (int threads : new int[] {
					// sync
					1,
					// async, low thread count
					2,
					// async, large thread count
					8 }) {
				// HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_VALUE_PARTIAL,
				List<String> modes = List.of(HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_VALUE_COMPLETE);
				for (String mode : modes) {
					String prefixes = String.join(";", "http://w1i.test.org/#Obj", "http://w2i.test.org/#Obj",
							"http://w3i.test.org/#Obj", "http://w4i.test.org/#Obj", "http://w5i.test.org/#Obj",
							"http://w6i.test.org/#Obj", "http://w7i.test.org/#Obj", "http://w8i.test.org/#Obj",
							"http://w9i.test.org/#Obj", "http://w10i.test.org/#Obj", "http://w11i.test.org/#Obj",
							"http://w12i.test.org/#Obj", "http://w13i.test.org/#Obj", "http://w14i.test.org/#Obj",
							"http://w15i.test.org/#Obj");
					params.addAll(List.of(
							new Object[] { "base-w" + threads + "-" + mode, SIZE_VALUE * 8, 20, 50, threads, mode,
									false, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_SECTION, SIZE_VALUE, "",
									prefixes },
							new Object[] { "duplicates-w" + threads + "-" + mode, SIZE_VALUE * 8, 10, 50, threads, mode,
									false, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_SECTION, SIZE_VALUE, "",
									prefixes },
							new Object[] { "large-literals-w" + threads + "-" + mode, SIZE_VALUE * 2, 20, 250, threads,
									mode, false, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_SECTION, SIZE_VALUE, "",
									prefixes },
							new Object[] { "quiet-w" + threads + "-" + mode, SIZE_VALUE * 8, 10, 50, threads, mode,
									false, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_SECTION, SIZE_VALUE, "",
									prefixes }));
				}
			}

			return params;
		}

		@Parameterized.Parameter
		public String name;
		@Parameterized.Parameter(1)
		public long maxSize;
		@Parameterized.Parameter(2)
		public int maxElementSplit;
		@Parameterized.Parameter(3)
		public int maxLiteralSize;
		@Parameterized.Parameter(4)
		public int threads;
		@Parameterized.Parameter(5)
		public String compressMode;
		@Parameterized.Parameter(6)
		public boolean quiet;
		@Parameterized.Parameter(7)
		public String dictionaryType;
		@Parameterized.Parameter(8)
		public long size;
		@Parameterized.Parameter(9)
		public String addedSpecs;
		@Parameterized.Parameter(10)
		public String prefixes;
		public boolean quadDict;

		@Before
		public void setupSpecs() {
			spec.setOptions(addedSpecs);
			spec.set(HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY, String.valueOf(threads));
			spec.set(HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_KEY, compressMode);
			spec.set(HDTOptionsKeys.DICTIONARY_TYPE_KEY, dictionaryType);
			spec.set(HDTOptionsKeys.LOADER_DISK_NO_COPY_ITERATOR_KEY, true);
			spec.set(HDTOptionsKeys.LOADER_PREFIXES, prefixes);

			quadDict = DictionaryFactory.isQuadDictionary(dictionaryType);
		}

		private void generateDiskTest() throws IOException, ParserException, NotFoundException, InterruptedException {
			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxSize(maxSize, SEED).withMaxElementSplit(maxElementSplit)
					.withMaxLiteralSize(maxLiteralSize).withSameTripleString(true).withUnicode(true)
					.withQuads(quadDict);

			if (spec.getBoolean("debug.disk.slow.stream")) {
				supplier.withSlowStream(25);
			}

			// create DISK HDT
			LargeFakeDataSetStreamSupplier.ThreadedStream genActual = supplier
					.createNTInputStream(CompressionType.GZIP);
			HDT actual = null;
			try {
				actual = HDTManager.generateHDTDisk(genActual.getStream(), HDTTestUtils.BASE_URI,
						quadDict ? RDFNotation.NQUAD : RDFNotation.NTRIPLES, CompressionType.GZIP, spec,
						quiet ? null : this);
				checkHDTConsistency(actual);
			} finally {
				if (actual == null) {
					genActual.getThread().interrupt();
				}
			}
			genActual.getThread().joinAndCrashIfRequired();

			supplier.reset();

			LargeFakeDataSetStreamSupplier.ThreadedStream genExpected = supplier
					.createNTInputStream(CompressionType.GZIP);
			// create MEMORY HDT
			HDT expected = null;
			try {
				expected = HDTManager.generateHDT(genExpected.getStream(), HDTTestUtils.BASE_URI,
						quadDict ? RDFNotation.NQUAD : RDFNotation.NTRIPLES, CompressionType.GZIP, spec, null);
				checkHDTConsistency(expected);
			} finally {
				if (expected == null) {
					genExpected.getThread().interrupt();
				}
			}
			genExpected.getThread().joinAndCrashIfRequired();

			// happy compiler, should throw before
			assertNotNull(expected);
			assertNotNull(actual);
			try {
				assertEqualsHDT(expected, actual);
			} finally {
				IOUtil.closeAll(expected, actual);
			}
		}

		@Test
		public void generateSaveLoadMapTest() throws IOException, ParserException, NotFoundException {
			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxSize(maxSize, SEED).withMaxElementSplit(maxElementSplit)
					.withMaxLiteralSize(maxLiteralSize).withUnicode(true)
					.withQuads(DictionaryFactory.isQuadDictionary(dictionaryType));

			// create MEMORY HDT

			try (HDT expected = HDTManager.generateHDT(supplier.createTripleStringStream(), HDTTestUtils.BASE_URI, spec,
					quiet ? null : this)) {
				String tmp = tempDir.newFile().getAbsolutePath();
				expected.saveToHDT(tmp, null);

				try (HDT mapExcepted = HDTManager.mapHDT(tmp, quiet ? null : this)) {
					assertEqualsHDT(expected, mapExcepted);
				}

				try (HDT loadExcepted = HDTManager.loadHDT(tmp, quiet ? null : this)) {
					assertEqualsHDT(expected, loadExcepted);
				}
			}

		}

		@Test
		public void generateDiskMemTest() throws IOException, ParserException, NotFoundException, InterruptedException {
			spec.set(HDTOptionsKeys.LOADER_DISK_CHUNK_SIZE_KEY, size);
			spec.set("debug.disk.build", true);
			generateDiskTest();
		}

		@Test
		public void generateDiskMapTest() throws IOException, ParserException, NotFoundException, InterruptedException {
			spec.set(HDTOptionsKeys.LOADER_DISK_CHUNK_SIZE_KEY, size);
			spec.set("debug.disk.build", true);
			Path mapHDT = tempDir.newFile("mapHDTTest.hdt").toPath();
			spec.set(HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY, mapHDT.toAbsolutePath());
			generateDiskTest();
			Files.deleteIfExists(mapHDT);
		}

		@Test
		public void catTreeTest() throws IOException, ParserException, NotFoundException, InterruptedException {
			Assume.assumeTrue(diskDictCat().contains(dictionaryType));
			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxSize(maxSize, SEED).withMaxElementSplit(maxElementSplit)
					.withMaxLiteralSize(maxLiteralSize).withUnicode(true).withQuads(quadDict);

			// create DISK HDT
			LargeFakeDataSetStreamSupplier.ThreadedStream genActual = supplier
					.createNTInputStream(CompressionType.NONE);
			HDT actual = null;
			try {
				actual = HDTManager.catTree(RDFFluxStop.sizeLimit(size), HDTSupplier.memory(), genActual.getStream(),
						HDTTestUtils.BASE_URI, quadDict ? RDFNotation.NQUAD : RDFNotation.NTRIPLES, spec,
						quiet ? null : this);
			} finally {
				if (actual == null) {
					genActual.getThread().interrupt();
				}
			}
			genActual.getThread().joinAndCrashIfRequired();

			supplier.reset();

			Iterator<TripleString> genExpected = supplier.createTripleStringStream();
			// create MEMORY HDT
			HDT expected = HDTManager.generateHDT(genExpected, HDTTestUtils.BASE_URI, spec, null);

			// happy compiler, should throw before
			assertNotNull(expected);
			assertNotNull(actual);
			try {
				assertEqualsHDT(expected, actual); // -1 for the original size
				// ignored by hdtcat
			} finally {
				IOUtil.closeAll(expected, actual);
			}
		}

		@Test
		public void catTreeDiskTest() throws IOException, ParserException, NotFoundException, InterruptedException {
			Assume.assumeTrue(diskDictCat().contains(dictionaryType));
			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxSize(maxSize, SEED).withMaxElementSplit(maxElementSplit)
					.withMaxLiteralSize(maxLiteralSize).withUnicode(true).withQuads(quadDict);

			spec.set("debug.disk.build", true);

			// create DISK HDT
			LargeFakeDataSetStreamSupplier.ThreadedStream genActual = supplier
					.createNTInputStream(CompressionType.NONE);
			HDT actual = null;
			try {
				actual = HDTManager.catTree(RDFFluxStop.sizeLimit(size), HDTSupplier.disk(), genActual.getStream(),
						HDTTestUtils.BASE_URI, quadDict ? RDFNotation.NQUAD : RDFNotation.NTRIPLES, spec,
						quiet ? null : this);
			} finally {
				if (actual == null) {
					genActual.getThread().interrupt();
				}
			}
			genActual.getThread().joinAndCrashIfRequired();

			supplier.reset();

			Iterator<TripleString> genExpected = supplier.createTripleStringStream();
			// create MEMORY HDT
			HDT expected = HDTManager.generateHDT(genExpected, HDTTestUtils.BASE_URI, spec, null);

			// happy compiler, should throw before
			assertNotNull(expected);
			assertNotNull(actual);
			try {
				assertEqualsHDT(expected, actual); // -1 for the original size
				// ignored by hdtcat
			} finally {
				IOUtil.closeAll(expected, actual);
			}
		}
	}

	@RunWith(Parameterized.class)
	public static class DynamicCatTreeTest extends HDTManagerTestBase {

		@Parameterized.Parameters(name = "{5} - {0} kcat: {8}(async:{9})")
		public static Collection<Object[]> params() {
			List<Object[]> params = new ArrayList<>();
			for (String dict : diskDict()) {
				for (boolean async : new boolean[] { false, true }) {
					for (long kcat : new long[] { 2, 10, 0 }) {
						params.add(
								new Object[] { "base", SIZE_VALUE * 16, 20, 50, false, dict, SIZE_VALUE, kcat, async });
						params.add(new Object[] { "duplicates", SIZE_VALUE * 16, 10, 50, false, dict, SIZE_VALUE, kcat,
								async });
						params.add(new Object[] { "large-literals", SIZE_VALUE * 4, 20, 250, false, dict, SIZE_VALUE,
								kcat, async });
						params.add(new Object[] { "quiet", SIZE_VALUE * 16, 10, 50, false, dict, SIZE_VALUE, kcat,
								async });
					}
				}
			}
			return params;
		}

		@Parameterized.Parameter
		public String name;
		@Parameterized.Parameter(1)
		public long maxSize;
		@Parameterized.Parameter(2)
		public int maxElementSplit;
		@Parameterized.Parameter(3)
		public int maxLiteralSize;
		@Parameterized.Parameter(4)
		public boolean quiet;
		@Parameterized.Parameter(5)
		public String dictionaryType;
		@Parameterized.Parameter(6)
		public long size;
		@Parameterized.Parameter(7)
		public long kCat;
		@Parameterized.Parameter(8)
		public boolean async;

		public boolean quadDict;

		@Before
		public void setupSpecs() {
			Assume.assumeTrue(diskDictCat().contains(dictionaryType));
			spec.set(HDTOptionsKeys.DICTIONARY_TYPE_KEY, dictionaryType);
			quadDict = DictionaryFactory.isQuadDictionary(dictionaryType);

			if (kCat != 0) {
				spec.set(HDTOptionsKeys.LOADER_CATTREE_KCAT, kCat);
			}
			if (async) {
				spec.set(HDTOptionsKeys.LOADER_CATTREE_ASYNC_KEY, true);
			}
		}

		@Test
		public void catTreeTest() throws IOException, ParserException, NotFoundException, InterruptedException {
			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxSize(maxSize, SEED).withMaxElementSplit(maxElementSplit)
					.withMaxLiteralSize(maxLiteralSize).withUnicode(true).withQuads(quadDict);

			// create DISK HDT
			LargeFakeDataSetStreamSupplier.ThreadedStream genActual = supplier
					.createNTInputStream(CompressionType.NONE);
			HDT actual = null;
			HDT expected = null;
			try {
				try {
					actual = HDTManager.catTree(RDFFluxStop.sizeLimit(size), HDTSupplier.memory(),
							genActual.getStream(), HDTTestUtils.BASE_URI,
							quadDict ? RDFNotation.NQUAD : RDFNotation.NTRIPLES, spec, quiet ? null : this);
				} finally {
					if (actual == null) {
						genActual.getThread().interrupt();
					}
				}
				genActual.getThread().joinAndCrashIfRequired();

				supplier.reset();

				Iterator<TripleString> genExpected = supplier.createTripleStringStream();
				// create MEMORY HDT
				expected = HDTManager.generateHDT(genExpected, HDTTestUtils.BASE_URI, spec, null);

				// happy compiler, should throw before
				assertNotNull(expected);
				assertNotNull(actual);
				assertEqualsHDT(expected, actual); // -1 for the original size
				// ignored by hdtcat
			} finally {
				IOUtil.closeAll(expected, actual);
			}
		}

		@Test
		public void catTreeMultipleTest() throws IOException, ParserException, NotFoundException, InterruptedException {
			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxSize(maxSize, SEED).withMaxElementSplit(maxElementSplit)
					.withMaxLiteralSize(maxLiteralSize).withUnicode(true).withQuads(quadDict);

			// create DISK HDT
			LargeFakeDataSetStreamSupplier.ThreadedStream genActual = supplier
					.createNTInputStream(CompressionType.NONE);
			HDTResult actual = null;
			HDT expected = null;
			final int maxFileCount = 10;
			Path root = tempDir.newFolder().toPath();
			try {
				try {
					HDTOptions spec = this.spec.pushTop();
					spec.set(HDTOptionsKeys.LOADER_CATTREE_MAX_FILES, maxFileCount);
					spec.set(HDTOptionsKeys.LOADER_CATTREE_KCAT, maxFileCount);
					spec.set(HDTOptionsKeys.HDTCAT_LOCATION, root);
					actual = HDTManager.catTreeMultiple(RDFFluxStop.sizeLimit(maxSize / maxFileCount * 3 / 5),
							HDTSupplier.memory(), genActual.getStream(), HDTTestUtils.BASE_URI,
							quadDict ? RDFNotation.NQUAD : RDFNotation.NTRIPLES, spec, quiet ? null : this);
				} finally {
					if (actual == null) {
						genActual.getThread().interrupt();
					}
				}
				genActual.getThread().joinAndCrashIfRequired();

				supplier.reset();

				Iterator<TripleString> genExpected = supplier.createTripleStringStream();
				// create MEMORY HDT
				expected = HDTManager.generateHDT(genExpected, HDTTestUtils.BASE_URI, spec, null);

				// happy compiler, should throw before
				assertNotNull(expected);
				assertNotNull(actual);

				assertTrue("not enough HDTs", actual.getHdtCount() >= 1);
				assertTrue("too much HDTs", actual.getHdtCount() <= maxFileCount);

				try (HDT actualHDT = combineHDTResult(actual, root)) {
					assertEqualsHDT(expected, actualHDT);
				}
				// ignored by hdtcat
			} finally {
				IOUtil.closeAll(expected, actual);
			}
		}

		@Test
		public void catTreeDiskTest() throws IOException, ParserException, NotFoundException, InterruptedException {
			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxSize(maxSize, SEED).withMaxElementSplit(maxElementSplit)
					.withMaxLiteralSize(maxLiteralSize).withUnicode(true).withQuads(quadDict);

			// create DISK HDT
			LargeFakeDataSetStreamSupplier.ThreadedStream genActual = supplier
					.createNTInputStream(CompressionType.NONE);
			HDT actual = null;
			try {
				actual = HDTManager.catTree(RDFFluxStop.sizeLimit(size), HDTSupplier.disk(), genActual.getStream(),
						HDTTestUtils.BASE_URI, quadDict ? RDFNotation.NQUAD : RDFNotation.NTRIPLES, spec,
						quiet ? null : this);
			} finally {
				if (actual == null) {
					genActual.getThread().interrupt();
				}
			}
			genActual.getThread().joinAndCrashIfRequired();

			supplier.reset();

			Iterator<TripleString> genExpected = supplier.createTripleStringStream();
			// create MEMORY HDT
			HDT expected = HDTManager.generateHDT(genExpected, HDTTestUtils.BASE_URI, spec, null);

			// happy compiler, should throw before
			assertNotNull(expected);
			assertNotNull(actual);
			try {
				checkHDTConsistency(expected);
				checkHDTConsistency(actual);
				assertEqualsHDT(expected, actual); // -1 for the original size
				// ignored by hdtcat
			} finally {
				IOUtil.closeAll(expected, actual);
			}
		}
	}

	@RunWith(Parameterized.class)
	public static class FileDynamicTest extends HDTManagerTestBase {
		@Parameterized.Parameters(name = "{0}")
		public static Collection<Object[]> params() {
			return List.of(new Object[] { "hdtGenDisk/unicode_disk_encode.nt", true, SIZE_VALUE },
					new Object[] { "unicodeTest.nt", true, SIZE_VALUE });
		}

		@Parameterized.Parameter
		public String file;
		@Parameterized.Parameter(1)
		public boolean quiet;
		@Parameterized.Parameter(2)
		public long size;

		private void generateDiskTest() throws IOException, ParserException, NotFoundException {
			String ntFile = Objects.requireNonNull(getClass().getClassLoader().getResource(file), "Can't find " + file)
					.getFile();
			// create DISK HDT
			try (HDT actual = HDTManager.generateHDTDisk(ntFile, HDTTestUtils.BASE_URI, RDFNotation.NTRIPLES, spec,
					quiet ? null : this)) {
				// create MEMORY HDT
				try (HDT expected = HDTManager.generateHDT(ntFile, HDTTestUtils.BASE_URI, RDFNotation.NTRIPLES, spec,
						null)) {
					checkHDTConsistency(actual);
					checkHDTConsistency(expected);
					assertEqualsHDT(expected, actual);
				}
			}
		}

		@Test
		public void generateDiskCompleteTest() throws IOException, ParserException, NotFoundException {
			spec.set(HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_KEY, CompressionResult.COMPRESSION_MODE_COMPLETE);
			spec.set(HDTOptionsKeys.LOADER_DISK_CHUNK_SIZE_KEY, size);
			spec.set("debug.disk.build", true);
			generateDiskTest();
		}

		@Test
		public void generateDiskPartialTest() throws IOException, ParserException, NotFoundException {
			spec.set(HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_KEY, CompressionResult.COMPRESSION_MODE_PARTIAL);
			spec.set(HDTOptionsKeys.LOADER_DISK_CHUNK_SIZE_KEY, size);
			spec.set("debug.disk.build", true);
			generateDiskTest();
		}

		@Test
		public void generateDiskCompleteMapTest() throws IOException, ParserException, NotFoundException {
			spec.set(HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_KEY, CompressionResult.COMPRESSION_MODE_COMPLETE);
			spec.set(HDTOptionsKeys.LOADER_DISK_CHUNK_SIZE_KEY, size);
			File mapHDT = tempDir.newFile("mapHDTTest.hdt");
			spec.set(HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY, mapHDT.getAbsolutePath());
			spec.set("debug.disk.build", true);
			generateDiskTest();
			Files.deleteIfExists(mapHDT.toPath());
		}

		@Test
		public void generateDiskPartialMapTest() throws IOException, ParserException, NotFoundException {
			spec.set(HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_KEY, CompressionResult.COMPRESSION_MODE_PARTIAL);
			spec.set(HDTOptionsKeys.LOADER_DISK_CHUNK_SIZE_KEY, size);
			File mapHDT = tempDir.newFile("mapHDTTest.hdt");
			spec.set(HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY, mapHDT.getAbsolutePath());
			generateDiskTest();
			Files.deleteIfExists(mapHDT.toPath());
		}

		@Test
		public void generateTest() throws IOException, ParserException, NotFoundException {
			String ntFile = Objects.requireNonNull(getClass().getClassLoader().getResource(file), "Can't find " + file)
					.getFile();
			// create DISK HDT
			try (InputStream in = IOUtil.getFileInputStream(ntFile)) {
				try (PipedCopyIterator<TripleString> it = RDFParserFactory.readAsIterator(
						RDFParserFactory.getParserCallback(RDFNotation.NTRIPLES,
								HDTOptions.of(Map.of(HDTOptionsKeys.NT_SIMPLE_PARSER_KEY, "true"))),
						in, HDTTestUtils.BASE_URI, true, RDFNotation.NTRIPLES)) {
					try (HDT expected = HDTManager.generateHDT(it, HDTTestUtils.BASE_URI, spec, quiet ? null : this)) {
						String testCopy = tempDir.newFile().getAbsolutePath();
						expected.saveToHDT(testCopy, null);

						// create MEMORY HDT
						try (HDT actual = HDTManager.loadHDT(testCopy)) {
							assertEqualsHDT(expected, actual);
						}
					}
				}
			}
		}
	}

	public static class StaticTest extends HDTManagerTestBase {
		@Test
		public void dirInjectionTest() throws Exception {
			Path root = tempDir.newFolder().toPath();

			int seed = 345678;
			int split = 10;
			long size = 500;

			try {

				LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
						.createSupplierWithMaxTriples(size, seed);

				HDTOptions gen = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
						HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG, HDTOptionsKeys.LOADER_TYPE_KEY,
						HDTOptionsKeys.LOADER_CATTREE_LOADERTYPE_KEY, HDTOptionsKeys.LOADER_CATTREE_LOCATION_KEY,
						root.resolve("work"), HDTOptionsKeys.LOADER_CATTREE_FUTURE_HDT_LOCATION_KEY,
						root.resolve("work.hdt"), HDTOptionsKeys.LOADER_CATTREE_LOADERTYPE_KEY, "disk",
						HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, root.resolve("workd"),
						HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY, root.resolve("workd.hdt"));

				Path didr = root.resolve("test");

				Files.createDirectories(didr);

				for (int i = 0; i < split; i++) {
					supplier.createNTFile(didr.resolve("d" + i + ".nt"));
				}

				LargeFakeDataSetStreamSupplier supplier2 = LargeFakeDataSetStreamSupplier
						.createSupplierWithMaxTriples(size * split, seed);
				Path exc = root.resolve("expected.hdt");
				supplier2.createAndSaveFakeHDT(gen, exc);

				Path actual = root.resolve("actual.hdt");
				try (HDT hdt = HDTManager.generateHDT(didr, LargeFakeDataSetStreamSupplier.BASE_URI, RDFNotation.DIR,
						gen, ProgressListener.ignore())) {
					hdt.saveToHDT(actual);
				}

				try (HDT actHDT = HDTManager.mapHDT(actual); HDT excHDT = HDTManager.mapHDT(exc)) {
					assertEqualsHDT(excHDT, actHDT);
				}
			} finally {
				PathUtils.deleteDirectory(root);
			}
		}

		@Test
		public void multiSectionTest() throws ParserException, IOException, NotFoundException {
			Path root = tempDir.newFolder().toPath();
			Path hdtFile = root.resolve("testhdt.hdt");
			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxTriples(10_000, 32).withMaxLiteralSize(30).withUnicode(true);

			// set MultiSectionDictionary type
			spec.set(HDTOptionsKeys.TEMP_DICTIONARY_IMPL_KEY, HDTOptionsKeys.TEMP_DICTIONARY_IMPL_VALUE_MULT_HASH);
			spec.set(HDTOptionsKeys.DICTIONARY_TYPE_KEY, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS);

			try (HDT hdt = HDTManager.generateHDT(supplier.createTripleStringStream(), HDTTestUtils.BASE_URI, spec,
					null)) {
				assertTrue(hdt.getDictionary() instanceof MultipleBaseDictionary);
				String testHdt = hdtFile.toString();
				hdt.saveToHDT(testHdt, null);

				// test mapping
				try (HDT hdt2 = HDTManager.mapHDT(testHdt)) {
					assertTrue(hdt2.getDictionary() instanceof MultipleBaseDictionary);
					assertEqualsHDT(hdt, hdt2);
				}
				// test loading
				try (HDT hdt2 = HDTManager.loadHDT(testHdt)) {
					assertTrue(hdt2.getDictionary() instanceof MultipleBaseDictionary);
					assertEqualsHDT(hdt, hdt2);
				}
			} finally {
				Files.deleteIfExists(hdtFile);
			}
			Path fakeNt = root.resolve("fake.nt");
			try {
				supplier.createNTFile(fakeNt);
				try (HDT hdt = HDTManager.generateHDT(fakeNt.toString(), HDTTestUtils.BASE_URI, RDFNotation.NTRIPLES,
						spec, null)) {
					String testHdt = hdtFile.toString();

					hdt.saveToHDT(testHdt, null);

					// test mapping
					try (HDT hdt2 = HDTManager.mapHDT(testHdt)) {
						assertTrue(hdt2.getDictionary() instanceof MultipleBaseDictionary);
						assertEqualsHDT(hdt, hdt2);
					}
					// test loading
					try (HDT hdt2 = HDTManager.loadHDT(testHdt)) {
						assertTrue(hdt2.getDictionary() instanceof MultipleBaseDictionary);
						assertEqualsHDT(hdt, hdt2);
					}
				}
			} finally {
				try {
					Files.deleteIfExists(fakeNt);
				} finally {
					Files.deleteIfExists(hdtFile);
				}
			}
		}

		@Test
		public void diffMultiSectTest() throws ParserException, IOException, NotFoundException {
			Path root = tempDir.newFolder().toPath();
			Path hdtFile = root.resolve("testhdt.hdt");
			Path diffLocation = root.resolve("diff");
			Files.createDirectories(diffLocation);
			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxTriples(10_000, 32).withMaxLiteralSize(30).withUnicode(true);

			// set MultiSectionDictionary type
			spec.set(HDTOptionsKeys.TEMP_DICTIONARY_IMPL_KEY, "multHash");
			spec.set(HDTOptionsKeys.DICTIONARY_TYPE_KEY, "dictionaryMultiObj");

			try (HDT hdt = HDTManager.generateHDT(supplier.createTripleStringStream(), HDTTestUtils.BASE_URI, spec,
					null)) {
				assertTrue(hdt.getDictionary() instanceof MultipleBaseDictionary);
				String testHdt = hdtFile.toString();
				hdt.saveToHDT(testHdt, null);

				ModifiableBitmap bitmap;

				// test mapping
				long n;
				try (HDT hdt2 = HDTManager.mapHDT(testHdt)) {
					bitmap = BitmapFactory.createRWBitmap(hdt2.getTriples().getNumberOfElements());
					assertTrue(hdt2.getDictionary() instanceof MultipleBaseDictionary);
					assertEqualsHDT(hdt, hdt2);

					n = hdt2.getTriples().getNumberOfElements();
				}

				Random rnd = new Random(SEED);
				for (long i = 0; i < n / 24; i++) {
					bitmap.set(Math.abs(rnd.nextLong()) % n, true);
				}

				try (HDT hdtDiff = HDTManager.diffHDTBit(diffLocation.toAbsolutePath().toString(), testHdt, bitmap,
						spec, null)) {
					assertEquals(hdt.getTriples().getNumberOfElements() - bitmap.countOnes(),
							hdtDiff.getTriples().getNumberOfElements());
				}
			} finally {
				try {
					Files.deleteIfExists(hdtFile);
				} finally {
					if (Files.exists(diffLocation)) {
						PathUtils.deleteDirectory(diffLocation);
					}
				}
			}
		}

		@Test
		public void calcErrorTest() throws ParserException, IOException, NotFoundException {
			Path root = tempDir.newFolder().toPath();

			HDTOptions s = HDTOptions.of(
					"loader.cattree.futureHDTLocation", root.resolve("cfuture.hdt"),
					"loader.cattree.loadertype", "disk",
					"loader.cattree.location", root.resolve("cattree"),
					"loader.cattree.memoryFaultFactor", "1",
					"loader.disk.futureHDTLocation", root.resolve("future_msd.hdt"),
					"loader.disk.location", root.resolve("gen"),
					"loader.type", "cat",
					"parser.ntSimpleParser", "true",
					"loader.disk.compressWorker", "3",
					"loader.cattree.kcat", "20",
					"hdtcat.location", root.resolve("catgen"),
					"hdtcat.location.future", root.resolve("catgen.hdt"),
					"bitmaptriples.sequence.disk", "true",
					"bitmaptriples.indexmethod", "disk",
					"bitmaptriples.sequence.disk.location", "bitmaptripleseq"
			);

			LargeFakeDataSetStreamSupplier sup = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(200000, 42)
					.withMaxElementSplit(100)
					.withMaxLiteralSize(20);

			Path outPath = root.resolve("t.hdt");

			long size;
			try (HDT hdt = HDTManager.generateHDT(sup.createTripleStringStream(), LargeFakeDataSetStreamSupplier.BASE_URI, s, ProgressListener.ignore())) {
				assertTrue(hdt instanceof MapOnCallHDT);
				size = hdt.getTriples().getNumberOfElements();
				hdt.saveToHDT(outPath);
			}

			try (HDT hdt = HDTManager.mapHDT(outPath)) {
				assertEquals(size, hdt.getTriples().getNumberOfElements());
			}

		}
	}

	@Ignore("handTests")
	public static class HandTest extends HDTManagerTestBase {
		@Test
		public void qzdqzdTest() throws ParserException, IOException {
			String path = "/Users/ate/workspace/qacompany/hdt-java-ate47/hdt-java-package/target/hdt-java-package-3.0.5-distribution/hdt-java-package-3.0.5/bin/shit.nt.gz";

			HDTSpecification spec = new HDTSpecification();
			spec.load(
					"/Users/ate/workspace/qacompany/hdt-java-ate47/hdt-java-package/target/hdt-java-package-3.0.5-distribution/hdt-java-package-3.0.5/bin/option.hdtspec");

			try (HDT hdt = HDTManager.generateHDTDisk(path, "http://ex.ogr/#", spec,
					(level, message) -> System.out.println("[" + level + "] " + message))) {
				System.out.println(hdt.getTriples().getNumberOfElements());
			}

		}

		@Test
		public void bigDiskTest() throws ParserException, IOException {
			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxSize(100_000_000L, 94);

			Path output = tempDir.newFolder().toPath();

			HDTOptions spec = new HDTSpecification();
			spec.set(HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY,
					output.resolve("future.hdt").toAbsolutePath().toString());
			spec.set(HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, output.resolve("gen_dir").toAbsolutePath().toString());
			spec.set(HDTOptionsKeys.NT_SIMPLE_PARSER_KEY, "true");
			spec.set(HDTOptionsKeys.PROFILER_KEY, "true");
			StopWatch watch = new StopWatch();
			watch.reset();
			try (HDT hdt = HDTManager.generateHDTDisk(supplier.createTripleStringStream(), "http://ex.ogr/#", spec,
					(level, message) -> System.out.println("[" + level + "] " + message))) {
				System.out.println(watch.stopAndShow());
				System.out.println(hdt.getTriples().getNumberOfElements());
			}
		}

		@Test
		public void bigCatTreeDiskTest() throws ParserException, IOException {
			HDTOptions spec = new HDTSpecification();
			StopWatch watch = new StopWatch();
			spec.set(HDTOptionsKeys.LOADER_CATTREE_LOCATION_KEY, "C:\\WIKI\\CATTREE\\WORKING");
			spec.set(HDTOptionsKeys.LOADER_CATTREE_FUTURE_HDT_LOCATION_KEY, "C:\\WIKI\\CATTREE\\future.hdt");
			spec.set(HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, "C:\\WIKI\\CATTREE\\WORKING_HDTDISK");
			spec.set(HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY, "12");
			spec.set(HDTOptionsKeys.NT_SIMPLE_PARSER_KEY, "true");
			spec.set(HDTOptionsKeys.PROFILER_KEY, "true");
			watch.reset();
			try (HDT hdt = HDTManager.catTree(RDFFluxStop.sizeLimit(100_000_000_000L) // 300GB
					// free
					.and(RDFFluxStop.countLimit(700_000_000L) // ~9GB maps
					), HDTSupplier.disk(), "M:\\WIKI\\latest-all.nt.bz2", HDTTestUtils.BASE_URI, RDFNotation.NTRIPLES,
					spec, (level, message) -> System.out.println("[" + level + "] " + message))) {
				System.out.println(watch.stopAndShow());
				System.out.println(hdt.getTriples().getNumberOfElements());
			}
		}

		@Test
		public void bigGenCatTreeDiskTest() throws ParserException, IOException {
			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxSize(10_000_000_000L, 94);
			HDTOptions spec = new HDTSpecification();
			StopWatch watch = new StopWatch();
			spec.set(HDTOptionsKeys.LOADER_CATTREE_LOCATION_KEY, "C:\\WIKI\\CATTREE\\WORKING");
			spec.set(HDTOptionsKeys.LOADER_CATTREE_FUTURE_HDT_LOCATION_KEY, "C:\\WIKI\\CATTREE\\future.hdt");
			spec.set(HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, "C:\\WIKI\\CATTREE\\WORKING_HDTDISK");
			spec.set(HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY, "12");
			spec.set(HDTOptionsKeys.NT_SIMPLE_PARSER_KEY, "true");
			spec.set(HDTOptionsKeys.PROFILER_KEY, "true");
			watch.reset();
			try (HDT hdt = HDTManager.catTree(RDFFluxStop.sizeLimit(100_000_000_000L) // 300GB
					// free
					.and(RDFFluxStop.countLimit(700_000_000L) // ~9GB maps
					), HDTSupplier.disk(), supplier.createTripleStringStream(), HDTTestUtils.BASE_URI, spec,
					(level, message) -> System.out.println("[" + level + "] " + message))) {
				System.out.println(watch.stopAndShow());
				System.out.println(hdt.getTriples().getNumberOfElements());
			}
		}

		@Test
		public void quadTest() throws IOException, ParserException {
			Path file = Path.of("C:\\Users\\wilat\\workspace\\hdtq\\trusty.LIDDIv1.01.trig");
			HDTOptions spec = HDTOptions
					.readFromFile(Path.of("C:\\Users\\wilat\\workspace\\hdtq\\qendpoint-cli-1.13.7\\bin\\opt.hdtspec"));

			long size = 100_000;
			int[] graph = { 10, 100, 1000, 10000, 25000, 50000 };

			for (int g : graph) {
				Path ff = file.resolveSibling("ds-" + g + ".nq.gz");
				LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(size, (int) (Math.tan(g) * 100))
						.withMaxElementSplit((int) (size / 500)).withMaxGraph(g).withQuads(true)
						.createNTFile(ff, CompressionType.GZIP);

				try (HDT hdt = HDTManager.generateHDT(ff, ff.toString().replace('\\', '/'), RDFNotation.NQUAD, spec,
						ProgressListener.sout())) {
					hdt.saveToHDT(ff.resolveSibling("big.hdtq"));
				}
			}
		}
	}

	@RunWith(Parameterized.class)
	public static class HDTQTest extends HDTManagerTestBase {
		@Parameterized.Parameters(name = "default graph:{0} type:{1}")
		public static Collection<Object[]> params() {
			List<Object[]> params = new ArrayList<>();

			for (String dict : List.of(HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_QUAD_SECTION,
					HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG_QUAD)) {
				for (boolean defaultGraph : List.of(true, false)) {
					params.add(new Object[] { defaultGraph, dict });
				}
			}

			return params;
		}

		@Parameterized.Parameter
		public boolean useDefaultGraph;
		@Parameterized.Parameter(1)
		public String dictType;

		private LargeFakeDataSetStreamSupplier createSupplier() {
			// fake data generation
			return LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(10000, 42)
					.withNoDefaultGraph(!useDefaultGraph).withQuads(true);
		}

		private void hdtqTesd(LargeFakeDataSetStreamSupplier supplier, Path d) throws NotFoundException, IOException {
			// run test
			Comparator<CharSequence> csc = CharSequenceComparator.getInstance();
			try (HDT h = HDTManager.mapIndexedHDT(d)) {
				checkHDTConsistency(h);
				Path indexFile = d.resolveSibling(d.getFileName() + HDTVersion.get_index_suffix("-"));
				assertTrue("can't find " + indexFile, Files.exists(indexFile));
				supplier.reset();
				Iterator<TripleString> it = supplier.createTripleStringStream();
				Set<TripleString> dataset = new HashSet<>();
				while (it.hasNext()) {
					dataset.add(it.next().tripleToString());
				}

				supplier.reset();
				long count = 0;
				for (TripleString ts : (Iterable<TripleString>) supplier::createTripleStringStream) {
					count++;
					TripleString tsstr = ts.tripleToString();
					assertTrue("can't find " + tsstr, dataset.contains(tsstr));
					CharSequence graph = ts.getGraph();
					if (graph.length() == 0) {
						IteratorTripleString it2 = h.search(ts.getSubject(), ts.getPredicate(), ts.getObject());

						// search until we have no graph
						while (true) {
							assertTrue(it2.hasNext());
							TripleString ts2 = it2.next();
							if (ts2.getGraph().length() == 0) {
								assertEquals(ts, ts2);
								break;
							}
						}
					} else {
						IteratorTripleString it2 = h.search(ts.getSubject(), ts.getPredicate(), ts.getObject(), graph);
						if (!it2.hasNext()) {
							BitmapTriplesIteratorPositionTest.printIterator(it2);
							fail("Can't find #" + count + " " + ts);
						}
						TripleString ts2 = it2.next();
						assertEquals(ts, ts2);
						if (it2.hasNext()) {
							BitmapTriplesIteratorPositionTest.printIterator(it2);
							System.err.println("***********");

							for (int i = 0; i < 5 && (i == 0 || it2.hasNext()); i++) {
								System.err.println(it2.next());
							}

							fail("Too many nodes for " + ts + " " + graph);
						}

						// empty search to check wildcard
						IteratorTripleString it3 = h.search(ts.getSubject(), ts.getPredicate(), ts.getObject(), "");
						while (true) {
							assertTrue(it3.hasNext());
							TripleString ts3 = it3.next();
							if (csc.compare(ts3.getGraph(), graph) == 0) {
								assertEquals(ts, ts3);
								break;
							}
						}
					}
				}

				assertEquals(dataset.size(), count);

				{
					IteratorTripleString itSearch = h.search("", "", "", "");
					long count2 = 0;
					while (itSearch.hasNext()) {
						count2++;
						TripleString ts = itSearch.next();
						TripleString tsstr = ts.tripleToString();
						assertTrue("can't find " + tsstr, dataset.contains(tsstr));

					}
					assertEquals(dataset.size(), count2);
				}

				// FOQ INDEX TEST

				StringBuilder roleDesc = new StringBuilder();
				for (TripleComponentRole role : TripleComponentRole.values()) {
					Set<TripleString> dataset2 = new HashSet<>(dataset);
					roleDesc.append(",").append(role);

					Iterator<? extends CharSequence> roleIt = h.getDictionary().stringIterator(role, true);

					long componentId = 0;
					Set<String> components = new HashSet<>();
					while (roleIt.hasNext()) {
						CharSequence component = roleIt.next();
						String str = component.toString();
						components.add(component.toString());
						long cid = componentId++;

						Iterator<TripleString> eid = switch (role) {
						case OBJECT -> h.search("", "", component, "");
						case SUBJECT -> h.search(component, "", "", "");
						case PREDICATE -> h.search("", component, "", "");
						case GRAPH -> h.search("", "", "", component);
						};

						long countEid = 0;
						while (eid.hasNext()) {
							TripleString tsstr = eid.next().tripleToString();
							countEid++;
							if (role == TripleComponentRole.GRAPH && !tsstr.getGraph().equals(str)) {
								// the default graph "" is searching all the
								// graphs, so we need
								// to check that we are using the right one.
								continue;
							}
							if (!dataset2.remove(tsstr)) {
								BitmapTriplesIteratorPositionTest.printIterator(eid);
								fail("can't remove " + tsstr + "\nfor " + role + "=" + component + "(" + cid + ")"
										+ "\ndone: " + roleDesc.substring(1) + "\n"
										+ String.join(",",
												components + "\nexists: " + dataset.contains(tsstr) + ", id: "
														+ countEid + "\npattern: "
														+ h.getDictionary().toTripleId(tsstr)));
							}
						}
					}
					assertTrue(dataset2.isEmpty());
				}

			}
		}

		@Test
		public void iteratorStreamGenerationTest() throws IOException, ParserException, NotFoundException {
			LargeFakeDataSetStreamSupplier supplier = createSupplier();
			Iterator<TripleString> it = supplier.createTripleStringStream();

			HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY, dictType);
			Path root = tempDir.newFolder().toPath();
			try {
				Path d = root.resolve("d.hdt");
				try (HDT hdt = HDTManager.generateHDT(it, HDTTestUtils.BASE_URI, spec, ProgressListener.ignore())) {
					hdt.saveToHDT(d.toAbsolutePath().toString(), ProgressListener.ignore());
				}
				hdtqTesd(supplier, d);
			} finally {
				PathUtils.deleteDirectory(root);
			}
		}

		@Test
		public void fileReadGenerationTest() throws IOException, ParserException, NotFoundException {
			LargeFakeDataSetStreamSupplier supplier = createSupplier();
			Iterator<TripleString> it = supplier.createTripleStringStream();

			HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY, dictType);
			Path root = tempDir.newFolder().toPath();
			try {
				Path nq = root.resolve("d.nq");
				try (BufferedWriter writer = Files.newBufferedWriter(nq)) {
					while (it.hasNext()) {
						it.next().dumpNtriple(writer);
					}
					writer.flush();
				}
				Path d = root.resolve("d.hdt");
				try (HDT hdt = HDTManager.generateHDT(nq.toAbsolutePath().toString(), HDTTestUtils.BASE_URI,
						RDFNotation.NQUAD, spec, ProgressListener.ignore())) {
					hdt.saveToHDT(d.toAbsolutePath().toString(), ProgressListener.ignore());
				}
				hdtqTesd(supplier, d);
			} finally {
				PathUtils.deleteDirectory(root);
			}
		}
	}

	public static class MSDLangTest extends HDTManagerTestBase {
		@Test
		public void msdLangTest() throws IOException, ParserException, NotFoundException {
			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(5000,
					34);
			Path ntFile = tempDir.newFile().toPath();
			try {

				supplier.createNTFile(ntFile);

				HDTOptions spec = HDTOptions.of(
						// use msdl
						HDTOptionsKeys.DICTIONARY_TYPE_KEY, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG);

				HDTOptions specFSD = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
						HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_SECTION);

				try (HDT hdt = HDTManager.generateHDT(ntFile, HDTTestUtils.BASE_URI, RDFNotation.NTRIPLES, spec,
						ProgressListener.ignore())) {
					Dictionary msdl = hdt.getDictionary();
					assertEquals(HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG, msdl.getType());
					assertTrue("not a msdl", msdl instanceof MultipleLangBaseDictionary);
					checkHDTConsistency(hdt);

					// the HDT is fine, does it contain all the triples?

					try (HDT hdtFSD = HDTManager.generateHDT(ntFile, HDTTestUtils.BASE_URI, RDFNotation.NTRIPLES,
							specFSD, ProgressListener.ignore())) {
						Dictionary fsd = hdtFSD.getDictionary();

						assertTrue("not a fsd", fsd instanceof BaseDictionary);
						assertEquals("not the same number of triples", hdtFSD.getTriples().getNumberOfElements(),
								hdt.getTriples().getNumberOfElements());
						assertEquals("Not the same number of SHARED", fsd.getNshared(), msdl.getNshared());
						assertEquals("Not the same number of SUBJECTS", fsd.getNsubjects(), msdl.getNsubjects());
						assertEquals("Not the same number of PREDICATES", fsd.getNpredicates(), msdl.getNpredicates());
						assertEquals("Not the same number of OBJECTS", fsd.getNobjects(), msdl.getNobjects());

						IteratorTripleString itMSDAT = hdt.search("", "", "");

						while (itMSDAT.hasNext()) {
							TripleString actual = itMSDAT.next();
							if (!hdt.search(actual).hasNext()) {
								fail(format("Can't find back triple %s in", actual));
							}
						}

						IteratorTripleString itMSDA = hdt.search("", "", "");

						while (itMSDA.hasNext()) {
							TripleString actual = itMSDA.next();

							IteratorTripleString itE = hdtFSD.search(actual);
							if (!itE.hasNext()) {
								long sid = fsd.stringToId(actual.getSubject(), SUBJECT);
								assertNotEquals("can't find SUB in FSD: " + actual.getSubject(), -1, sid);
								long pid = fsd.stringToId(actual.getPredicate(), TripleComponentRole.PREDICATE);
								assertNotEquals("can't find PRE in FSD: " + actual.getPredicate(), -1, pid);
								long oid = fsd.stringToId(actual.getObject(), OBJECT);
								assertNotEquals("can't find OBJ in FSD: " + actual.getObject(), -1, oid);

								assertEquals(actual.getSubject().toString(), fsd.idToString(sid, SUBJECT).toString());
								assertEquals(actual.getPredicate().toString(),
										fsd.idToString(pid, TripleComponentRole.PREDICATE).toString());
								assertEquals(actual.getObject().toString(), fsd.idToString(oid, OBJECT).toString());

								fail(format("Can't find triple %s in FSD", actual));
							}
							assertEquals(actual.tripleToString(), itE.next().tripleToString());
						}

						IteratorTripleString itE = hdtFSD.search("", "", "");

						while (itE.hasNext()) {
							TripleString excepted = itE.next();
							IteratorTripleString itA = hdt.search(excepted.getSubject(), excepted.getPredicate(),
									excepted.getObject());
							if (!itA.hasNext()) {
								long sid = msdl.stringToId(excepted.getSubject(), SUBJECT);
								assertNotEquals("can't find SUB in MSDL: " + excepted.getSubject(), -1, sid);
								long pid = msdl.stringToId(excepted.getPredicate(), TripleComponentRole.PREDICATE);
								assertNotEquals("can't find PRE in MSDL: " + excepted.getPredicate(), -1, pid);
								long oid = msdl.stringToId(excepted.getObject(), OBJECT);
								assertNotEquals("can't find OBJ in MSDL: " + excepted.getObject(), -1, oid);

								assertEquals(excepted.getSubject().toString(),
										msdl.idToString(sid, SUBJECT).toString());
								assertEquals(excepted.getPredicate().toString(),
										msdl.idToString(pid, TripleComponentRole.PREDICATE).toString());
								assertEquals(excepted.getObject().toString(), msdl.idToString(oid, OBJECT).toString());

								TripleID tid = new TripleID(sid, pid, oid);
								IteratorTripleID itA2 = hdt.getTriples().search(tid);
								if (itA2.hasNext()) {
									fail(format("can't find triple %s by string in MSDL HDT", excepted));
								} else {
									fail(format("can't find triple %s by string or id in MSDL HDT (%s)", excepted,
											tid));
								}

							}
							TripleString actual = itA.next();
							assertComponentsNotNull("an element is null", actual);
							assertEquals(excepted, actual);
						}
					}

					// try to load/map the HDT

					Path tempHDT = tempDir.newFile().toPath();
					try {
						hdt.saveToHDT(tempHDT, ProgressListener.ignore());
						try (HDT hdtMap = HDTManager.mapHDT(tempHDT)) {
							assertEquals(HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG,
									hdtMap.getDictionary().getType());
							assertEqualsHDT(hdt, hdtMap);
							try (HDT hdtLoad = HDTManager.loadHDT(tempHDT)) {
								assertEquals(HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG,
										hdtLoad.getDictionary().getType());
								assertEqualsHDT(hdt, hdtLoad);
								assertEqualsHDT(hdtLoad, hdtMap);
							}
						}
					} finally {
						Files.deleteIfExists(tempHDT);
					}
				}
			} finally {
				Files.deleteIfExists(ntFile);
			}
		}

		@Test
		public void diskMsdLangMemTest() throws IOException, ParserException, NotFoundException {
			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(5000,
					34);
			Path rootDir = tempDir.newFolder().toPath();
			try {
				Path ntFile = rootDir.resolve("ds.nt");

				supplier.createNTFile(ntFile);

				HDTOptions spec = HDTOptions.of(
						// use msdl
						HDTOptionsKeys.DICTIONARY_TYPE_KEY, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG,
						// use GD
						HDTOptionsKeys.LOADER_TYPE_KEY, HDTOptionsKeys.LOADER_TYPE_VALUE_DISK,

						HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, rootDir.resolve("gd"));

				HDTOptions specMem = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
						HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG);

				try (HDT hdtGD = HDTManager.generateHDT(ntFile, HDTTestUtils.BASE_URI, RDFNotation.NTRIPLES, spec,
						ProgressListener.ignore())) {
					Dictionary msdlGD = hdtGD.getDictionary();
					assertEquals(HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG, msdlGD.getType());
					assertTrue("not a msdl", msdlGD instanceof MultipleLangBaseDictionary);
					checkHDTConsistency(hdtGD);

					// the HDT is fine, does it contain all the triples?

					try (HDT hdtMem = HDTManager.generateHDT(ntFile, HDTTestUtils.BASE_URI, RDFNotation.NTRIPLES,
							specMem, ProgressListener.ignore())) {
						Dictionary msdlMem = hdtMem.getDictionary();

						assertTrue("not a msdl", msdlMem instanceof MultipleLangBaseDictionary);
						assertEquals("not the same number of triples", hdtMem.getTriples().getNumberOfElements(),
								hdtGD.getTriples().getNumberOfElements());
						assertEquals("Not the same number of SHARED", msdlMem.getNshared(), msdlGD.getNshared());
						assertEquals("Not the same number of SUBJECTS", msdlMem.getNsubjects(), msdlGD.getNsubjects());
						assertEquals("Not the same number of PREDICATES", msdlMem.getNpredicates(),
								msdlGD.getNpredicates());
						assertEquals("Not the same number of OBJECTS", msdlMem.getNobjects(), msdlGD.getNobjects());

						assertEqualsHDT(hdtMem, hdtGD);
					}

					// try to load/map the HDT

					Path tempHDT = rootDir.resolve("testmsdl.hdt");

					hdtGD.saveToHDT(tempHDT, ProgressListener.ignore());
					try (HDT hdtMap = HDTManager.mapHDT(tempHDT)) {
						assertEquals(HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG, hdtMap.getDictionary().getType());
						assertEqualsHDT(hdtGD, hdtMap);
						try (HDT hdtLoad = HDTManager.loadHDT(tempHDT)) {
							assertEquals(HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG,
									hdtLoad.getDictionary().getType());
							assertEqualsHDT(hdtGD, hdtLoad);
							assertEqualsHDT(hdtLoad, hdtMap);
						}
					}
				}
			} finally {
				PathUtils.deleteDirectory(rootDir);
			}
		}

		@Test
		public void diskMsdLangMapTest() throws IOException, ParserException, NotFoundException {
			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(5000,
					34);
			Path rootDir = tempDir.newFolder().toPath();
			try {
				Path ntFile = rootDir.resolve("ds.nt");

				supplier.createNTFile(ntFile);

				HDTOptions spec = HDTOptions.of(
						// use msdl
						HDTOptionsKeys.DICTIONARY_TYPE_KEY, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG,
						// use GD
						HDTOptionsKeys.LOADER_TYPE_KEY, HDTOptionsKeys.LOADER_TYPE_VALUE_DISK,

						HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, rootDir.resolve("gd"),

						HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY, rootDir.resolve("future.hdt"));

				HDTOptions specMem = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
						HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG);

				try (HDT hdtGD = HDTManager.generateHDT(ntFile, HDTTestUtils.BASE_URI, RDFNotation.NTRIPLES, spec,
						ProgressListener.ignore())) {
					Dictionary msdlGD = hdtGD.getDictionary();
					assertEquals(HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG, msdlGD.getType());
					assertTrue("not a msdl", msdlGD instanceof MultipleLangBaseDictionary);
					checkHDTConsistency(hdtGD);

					// the HDT is fine, does it contain all the triples?

					try (HDT hdtMem = HDTManager.generateHDT(ntFile, HDTTestUtils.BASE_URI, RDFNotation.NTRIPLES,
							specMem, ProgressListener.ignore())) {
						Dictionary msdlMem = hdtMem.getDictionary();

						assertTrue("not a msdl", msdlMem instanceof MultipleLangBaseDictionary);
						assertEquals("not the same number of triples", hdtMem.getTriples().getNumberOfElements(),
								hdtGD.getTriples().getNumberOfElements());
						assertEquals("Not the same number of SHARED", msdlMem.getNshared(), msdlGD.getNshared());
						assertEquals("Not the same number of SUBJECTS", msdlMem.getNsubjects(), msdlGD.getNsubjects());
						assertEquals("Not the same number of PREDICATES", msdlMem.getNpredicates(),
								msdlGD.getNpredicates());
						assertEquals("Not the same number of OBJECTS", msdlMem.getNobjects(), msdlGD.getNobjects());

						assertEqualsHDT(hdtMem, hdtGD);
					}

					// try to load/map the HDT

					Path tempHDT = rootDir.resolve("testmsdl.hdt");

					hdtGD.saveToHDT(tempHDT, ProgressListener.ignore());
					try (HDT hdtMap = HDTManager.mapHDT(tempHDT)) {
						assertEquals(HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG, hdtMap.getDictionary().getType());
						assertEqualsHDT(hdtGD, hdtMap);
						try (HDT hdtLoad = HDTManager.loadHDT(tempHDT)) {
							assertEquals(HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG,
									hdtLoad.getDictionary().getType());
							assertEqualsHDT(hdtGD, hdtLoad);
							assertEqualsHDT(hdtLoad, hdtMap);
						}
					}
				}
			} finally {
				PathUtils.deleteDirectory(rootDir);
			}
		}

		@Test
		public void msdLangCatTest() throws IOException, ParserException, NotFoundException {
			Path root = tempDir.newFolder().toPath();
			try {
				final int sub = 3;
				final long count = 2500;

				LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
						.createSupplierWithMaxTriples(count, 34);
				LargeFakeDataSetStreamSupplier supplier2 = LargeFakeDataSetStreamSupplier
						.createSupplierWithMaxTriples(count * sub, 34);
				String base = "sub";

				Path ng = root.resolve("ng.nt");
				Path hdtg = root.resolve("hg.hdt");

				List<Path> ngs = new ArrayList<>();

				supplier2.createNTFile(ng);

				HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
						HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG);

				for (int i = 0; i < sub + 1; i++) {
					Path in;
					Path out;
					if (i == 0) {
						in = ng;
						out = hdtg;
					} else {
						in = root.resolve(base + i + ".nt");
						out = root.resolve(base + i + ".hdt");
						ngs.add(out);
						supplier.createNTFile(in);
					}
					try (HDT h = HDTManager.generateHDT(in, HDTTestUtils.BASE_URI, RDFNotation.NTRIPLES, spec,
							ProgressListener.ignore())) {
						h.saveToHDT(out);
					}
				}

				// ngs contains the list of the HDT to cat

				HDTOptions specCat = spec.pushTop();

				specCat.setOptions(HDTOptionsKeys.HDTCAT_LOCATION, root.resolve("khc"),

						HDTOptionsKeys.HDTCAT_FUTURE_LOCATION, root.resolve("khc.hdt"));

				try (HDT catOut = HDTManager.catHDTPath(ngs, specCat, ProgressListener.ignore())) {
					try (HDT excepted = HDTManager.mapHDT(hdtg)) {
						assertEqualsHDT(excepted, catOut);
					}
				}

			} finally {
				PathUtils.deleteDirectory(root);
			}
		}

		@Test
		public void idFromIteratorTest() throws IOException, ParserException {
			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxTriples(5000, 34).withMaxLiteralSize(50).withMaxElementSplit(20);
			Path rootDir = tempDir.newFolder().toPath();
			try {
				Path hdtPath = rootDir.resolve("ds.nt");

				HDTOptions spec = HDTOptions.of(
						// use msdl
						HDTOptionsKeys.DICTIONARY_TYPE_KEY, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG,
						// use GD
						HDTOptionsKeys.LOADER_TYPE_KEY, HDTOptionsKeys.LOADER_TYPE_VALUE_DISK,

						HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, rootDir.resolve("gd"),

						HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY, rootDir.resolve("future.hdt"));

				supplier.createAndSaveFakeHDT(spec, hdtPath);

				try (HDT hdt = HDTManager.mapHDT(hdtPath)) {
					Dictionary dictUkn = hdt.getDictionary();

					if (!(dictUkn instanceof MultipleLangBaseDictionary dict)) {
						fail("bad dict type: %s".formatted(dictUkn.getClass()));
						return;
					}

					assertTrue(dict.supportsDataTypeOfId());
					assertTrue(dict.supportsLanguageOfId());
					assertTrue(dict.supportsNodeTypeOfId());

					for (TripleComponentRole role : TripleComponentRole.valuesNoGraph()) {
						long idc = 1;
						Iterator<? extends CharSequence> it = dict.stringIterator(role, true);

						while (it.hasNext()) {
							CharSequence component = it.next();
							long id = idc++;

							CharSequence componentActual = dict.idToString(id, role);

							if (!component.toString().equals(componentActual.toString())) {
								fail("%s != %s for id %d/%s".formatted(component, componentActual, id, role));
							}
						}
					}
					Set<ByteString> loaded = new HashSet<>();
					for (TripleComponentRole role : new TripleComponentRole[] { SUBJECT, OBJECT }) {
						long nshared = dict.getNshared();
						long idc = 1;
						Iterator<? extends CharSequence> it = dict.stringIterator(role, true);

						while (it.hasNext()) {
							CharSequence component = it.next();
							long id = idc++;

							if (!loaded.add(ByteString.of(component))) {
								if (id > nshared) { // normal for shared
									fail(format("the component %s(%s/%d) was loaded twice! ", component, role, id));
								}
							}

							assertEquals("bad id mapping", id, dict.stringToId(component, role));

							CharSequence componentActual = dict.idToString(id, role);
							assertEquals("bad string mapping", component.toString(), componentActual.toString());

							TripleComponentRole role2 = role == SUBJECT ? OBJECT : SUBJECT;

							if (id <= nshared) {
								assertEquals("bad role logic", id, dict.stringToId(component, role2));
							} else {
								assertTrue("bad role logic", dict.stringToId(component, role2) <= 0);
							}

							RDFNodeType nodeType = RDFNodeType.typeof(component);

							RDFNodeType actualNodeType = dict.nodeTypeOfId(role, id);
							if (nodeType != actualNodeType) {
								StringBuilder bld = new StringBuilder("Sections: ");
								for (int i = 0; i < dict.getObjectsSectionCount(); i++) {
									MultipleLangBaseDictionary.ObjectIdLocationData sec = dict
											.getObjectsSectionFromId(i);
									bld.append("%d=%s(%s)\n".formatted(sec.location(), sec.name(), sec.type()));
								}
								fail("bad node type %s != %s for %s (%s/%d@%d)\n%s".formatted(nodeType, actualNodeType,
										component, role, id, nshared, bld));
							}
							if (role == OBJECT) {
								CharSequence lang = LiteralsUtils.getLanguage(component).orElse(null);
								assertEquals("bad lang", lang, dict.languageOfId(id));

								CharSequence type = LiteralsUtils.getType(component);
								assertEquals("bad type", type, dict.dataTypeOfId(id));
							}
						}
					}
				}
			} finally {
				PathUtils.deleteDirectory(rootDir);
			}
		}
	}

	public static class MSDLangQuadTest extends HDTManagerTestBase {
		@Test
		public void msdLangTest() throws Exception {
			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxTriples(5000, 34).withQuads(true);
			Path ntFile = tempDir.newFile().toPath();
			try {

				supplier.createNTFile(ntFile);

				HDTOptions spec = HDTOptions.of(
						// use msdl
						HDTOptionsKeys.DICTIONARY_TYPE_KEY,
						HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG_QUAD);

				HDTOptions specFSD = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
						HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_QUAD_SECTION);

				try (HDT hdt = HDTManager.generateHDT(ntFile, HDTTestUtils.BASE_URI, RDFNotation.NQUAD, spec,
						ProgressListener.ignore())) {
					Dictionary msdl = hdt.getDictionary();
					assertEquals(HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG_QUAD, msdl.getType());
					assertTrue("not a msdlq", msdl instanceof MultipleLangBaseDictionary);
					checkHDTConsistency(hdt);

					// the HDT is fine, does it contain all the triples?

					try (HDT hdtFSD = HDTManager.generateHDT(ntFile, HDTTestUtils.BASE_URI, RDFNotation.NQUAD, specFSD,
							ProgressListener.ignore())) {
						Dictionary fsd = hdtFSD.getDictionary();

						assertTrue("not a fsd", fsd instanceof BaseDictionary);
						assertEquals("not the same number of triples", hdtFSD.getTriples().getNumberOfElements(),
								hdt.getTriples().getNumberOfElements());
						assertEquals("Not the same number of SHARED", fsd.getNshared(), msdl.getNshared());
						assertEquals("Not the same number of SUBJECTS", fsd.getNsubjects(), msdl.getNsubjects());
						assertEquals("Not the same number of PREDICATES", fsd.getNpredicates(), msdl.getNpredicates());
						assertEquals("Not the same number of OBJECTS", fsd.getNobjects(), msdl.getNobjects());

						IteratorTripleString itMSDAT = hdt.search("", "", "");

						while (itMSDAT.hasNext()) {
							TripleString actual = itMSDAT.next();
							if (!hdt.search(actual).hasNext()) {
								fail(format("Can't find back triple %s in", actual));
							}
						}

						IteratorTripleString itMSDA = hdt.search("", "", "");

						while (itMSDA.hasNext()) {
							TripleString actual = itMSDA.next();

							IteratorTripleString itE = hdtFSD.search(actual);
							if (!itE.hasNext()) {
								long sid = fsd.stringToId(actual.getSubject(), SUBJECT);
								assertNotEquals("can't find SUB in FSD: " + actual.getSubject(), -1, sid);
								long pid = fsd.stringToId(actual.getPredicate(), TripleComponentRole.PREDICATE);
								assertNotEquals("can't find PRE in FSD: " + actual.getPredicate(), -1, pid);
								long oid = fsd.stringToId(actual.getObject(), OBJECT);
								assertNotEquals("can't find OBJ in FSD: " + actual.getObject(), -1, oid);

								assertEquals(actual.getSubject().toString(), fsd.idToString(sid, SUBJECT).toString());
								assertEquals(actual.getPredicate().toString(),
										fsd.idToString(pid, TripleComponentRole.PREDICATE).toString());
								assertEquals(actual.getObject().toString(), fsd.idToString(oid, OBJECT).toString());

								fail(format("Can't find triple %s in FSD", actual));
							}
							assertEquals(actual.tripleToString(), itE.next().tripleToString());
						}

						IteratorTripleString itE = hdtFSD.search("", "", "");

						while (itE.hasNext()) {
							TripleString excepted = itE.next();
							IteratorTripleString itA = hdt.search(excepted.getSubject(), excepted.getPredicate(),
									excepted.getObject());
							if (!itA.hasNext()) {
								long sid = msdl.stringToId(excepted.getSubject(), SUBJECT);
								assertNotEquals("can't find SUB in MSDL: " + excepted.getSubject(), -1, sid);
								long pid = msdl.stringToId(excepted.getPredicate(), TripleComponentRole.PREDICATE);
								assertNotEquals("can't find PRE in MSDL: " + excepted.getPredicate(), -1, pid);
								long oid = msdl.stringToId(excepted.getObject(), OBJECT);
								assertNotEquals("can't find OBJ in MSDL: " + excepted.getObject(), -1, oid);

								assertEquals(excepted.getSubject().toString(),
										msdl.idToString(sid, SUBJECT).toString());
								assertEquals(excepted.getPredicate().toString(),
										msdl.idToString(pid, TripleComponentRole.PREDICATE).toString());
								assertEquals(excepted.getObject().toString(), msdl.idToString(oid, OBJECT).toString());

								TripleID tid = new TripleID(sid, pid, oid);
								IteratorTripleID itA2 = hdt.getTriples().search(tid);
								if (itA2.hasNext()) {
									fail(format("can't find triple %s by string in MSDL HDT", excepted));
								} else {
									fail(format("can't find triple %s by string or id in MSDL HDT (%s)", excepted,
											tid));
								}

							}
							TripleString actual = itA.next();
							assertComponentsNotNull("an element is null", actual);
							assertEquals(excepted, actual);
						}
					}

					// try to load/map the HDT

					Path tempHDT = tempDir.newFile().toPath();
					try {
						hdt.saveToHDT(tempHDT, ProgressListener.ignore());
						try (HDT hdtMap = HDTManager.mapHDT(tempHDT)) {
							assertEquals(HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG_QUAD,
									hdtMap.getDictionary().getType());
							assertEqualsHDT(hdt, hdtMap);
							try (HDT hdtLoad = HDTManager.loadHDT(tempHDT)) {
								assertEquals(HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG_QUAD,
										hdtLoad.getDictionary().getType());
								assertEqualsHDT(hdt, hdtLoad);
								assertEqualsHDT(hdtLoad, hdtMap);
							}
						}
					} catch (Throwable t) {
						try {
							Files.deleteIfExists(tempHDT);
						} catch (IOException e) {
							t.addSuppressed(e);
						}
						throw t;
					}
					Files.deleteIfExists(tempHDT);
				}
			} catch (Throwable t) {
				try {
					Files.deleteIfExists(ntFile);
				} catch (IOException e) {
					t.addSuppressed(e);
				}
				throw t;
			}
			Files.deleteIfExists(ntFile);
		}

		@Test
		public void idFromIteratorTest() throws IOException, ParserException {
			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxTriples(5000, 34).withMaxLiteralSize(50).withMaxElementSplit(20)
					.withQuads(true);
			Path rootDir = tempDir.newFolder().toPath();
			try {
				Path hdtPath = rootDir.resolve("ds.nt");

				HDTOptions spec = HDTOptions.of(
						// use msdl
						HDTOptionsKeys.DICTIONARY_TYPE_KEY, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG,
						// use GD
						HDTOptionsKeys.LOADER_TYPE_KEY, HDTOptionsKeys.LOADER_TYPE_VALUE_DISK,

						HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, rootDir.resolve("gd"),

						HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY, rootDir.resolve("future.hdt"));

				supplier.createAndSaveFakeHDT(spec, hdtPath);

				try (HDT hdt = HDTManager.mapHDT(hdtPath)) {
					Dictionary dictUkn = hdt.getDictionary();

					if (!(dictUkn instanceof MultipleLangBaseDictionary dict)) {
						fail("bad dict type: %s".formatted(dictUkn.getClass()));
						return;
					}

					assertTrue(dict.supportsDataTypeOfId());
					assertTrue(dict.supportsLanguageOfId());
					assertTrue(dict.supportsNodeTypeOfId());

					for (TripleComponentRole role : TripleComponentRole.valuesNoGraph()) {
						long idc = 1;
						Iterator<? extends CharSequence> it = dict.stringIterator(role, true);

						while (it.hasNext()) {
							CharSequence component = it.next();
							long id = idc++;

							CharSequence componentActual = dict.idToString(id, role);

							if (!component.toString().equals(componentActual.toString())) {
								fail("%s != %s for id %d/%s".formatted(component, componentActual, id, role));
							}
						}
					}
					Set<ByteString> loaded = new HashSet<>();
					for (TripleComponentRole role : new TripleComponentRole[] { SUBJECT, OBJECT }) {
						long nshared = dict.getNshared();
						long idc = 1;
						Iterator<? extends CharSequence> it = dict.stringIterator(role, true);

						while (it.hasNext()) {
							CharSequence component = it.next();
							long id = idc++;

							if (!loaded.add(ByteString.of(component))) {
								if (id > nshared) { // normal for shared
									fail(format("the component %s(%s/%d) was loaded twice! ", component, role, id));
								}
							}

							assertEquals("bad id mapping", id, dict.stringToId(component, role));

							CharSequence componentActual = dict.idToString(id, role);
							assertEquals("bad string mapping", component.toString(), componentActual.toString());

							TripleComponentRole role2 = role == SUBJECT ? OBJECT : SUBJECT;

							if (id <= nshared) {
								assertEquals("bad role logic", id, dict.stringToId(component, role2));
							} else {
								assertTrue("bad role logic", dict.stringToId(component, role2) <= 0);
							}

							RDFNodeType nodeType = RDFNodeType.typeof(component);

							RDFNodeType actualNodeType = dict.nodeTypeOfId(role, id);
							if (nodeType != actualNodeType) {
								StringBuilder bld = new StringBuilder("Sections: ");
								for (int i = 0; i < dict.getObjectsSectionCount(); i++) {
									MultipleLangBaseDictionary.ObjectIdLocationData sec = dict
											.getObjectsSectionFromId(i);
									bld.append("%d=%s(%s)\n".formatted(sec.location(), sec.name(), sec.type()));
								}
								fail("bad node type %s != %s for %s (%s/%d@%d)\n%s".formatted(nodeType, actualNodeType,
										component, role, id, nshared, bld));
							}
							if (role == OBJECT) {
								CharSequence lang = LiteralsUtils.getLanguage(component).orElse(null);
								assertEquals("bad lang", lang, dict.languageOfId(id));

								CharSequence type = LiteralsUtils.getType(component);
								assertEquals("bad type", type, dict.dataTypeOfId(id));
							}
						}
					}
				}
			} finally {
				PathUtils.deleteDirectory(rootDir);
			}
		}
	}

	@RunWith(Parameterized.class)
	public static class DictionaryLangTypeTest extends HDTManagerTestBase {

		@Parameterized.Parameters(name = "dict:{0}")
		public static Collection<String> params() {
			return diskDict();
		}

		@Parameterized.Parameter
		public String dictType;

		@Test
		public void msdLangTypeFetchTest() throws IOException, ParserException {
			Path root = tempDir.newFolder().toPath();
			try {
				final long count = 2500;

				LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
						.createSupplierWithMaxTriples(count, 34).withMaxElementSplit(20).withMaxLiteralSize(10)
						.withUnicode(false);

				Path hdtg = root.resolve("hg.hdt");

				try (HDT hdt = supplier.createFakeHDT(HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY, dictType))) {
					if (!hdt.getDictionary().supportsDataTypeOfId() && !hdt.getDictionary().supportsLanguageOfId()
							&& !hdt.getDictionary().supportsNodeTypeOfId()) {
						logger.debug("This dictionary doesn't support datatype/language/rdf-type retrieve");
						return;
					}
					hdt.saveToHDT(hdtg);
				}

				try (HDT msdl = HDTManager.mapHDT(hdtg)) {
					Iterator<TripleID> it = msdl.getTriples().searchAll();
					while (it.hasNext()) {
						TripleID ts = it.next();

						long oid = ts.getObject();
						CharSequence obj = msdl.getDictionary().idToString(oid, OBJECT);

						assertNotNull("obj is null", obj);

						CharSequence type = LiteralsUtils.getType(obj);

						if (msdl.getDictionary().supportsDataTypeOfId()) {
							assertEquals(type.toString(), msdl.getDictionary().dataTypeOfId(oid).toString());
						}

						if (msdl.getDictionary().supportsLanguageOfId() && type == LiteralsUtils.LITERAL_LANG_TYPE) {
							CharSequence lang = LiteralsUtils.getLanguage(obj)
									.orElseThrow(() -> new AssertionError("No lang"));

							String langActual = msdl.getDictionary().languageOfId(oid).toString();

							assertEquals(lang.toString(), langActual);
						}

						if (msdl.getDictionary().supportsNodeTypeOfId()) {
							CharSequence subj = msdl.getDictionary().idToString(ts.getSubject(), SUBJECT);
							CharSequence pred = msdl.getDictionary().idToString(ts.getPredicate(),
									TripleComponentRole.PREDICATE);

							RDFNodeType stype = msdl.getDictionary().nodeTypeOfId(SUBJECT, ts.getSubject());
							RDFNodeType ptype = msdl.getDictionary().nodeTypeOfId(TripleComponentRole.PREDICATE,
									ts.getPredicate());
							RDFNodeType otype = msdl.getDictionary().nodeTypeOfId(OBJECT, ts.getObject());

							assertEquals(String.valueOf(ts.getSubject()), RDFNodeType.typeof(subj), stype);
							assertEquals(String.valueOf(ts.getPredicate()), RDFNodeType.typeof(pred), ptype);
							assertEquals(String.valueOf(ts.getObject()), RDFNodeType.typeof(obj), otype);

						}
					}
				}
			} finally {
				PathUtils.deleteDirectory(root);
			}
		}
	}
}
