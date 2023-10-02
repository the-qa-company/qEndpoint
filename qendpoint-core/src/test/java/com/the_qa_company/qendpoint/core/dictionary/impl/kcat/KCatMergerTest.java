package com.the_qa_company.qendpoint.core.dictionary.impl.kcat;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.compact.bitmap.BitmapFactory;
import com.the_qa_company.qendpoint.core.compact.bitmap.GraphDeleteBitmap;
import com.the_qa_company.qendpoint.core.compact.bitmap.ModifiableBitmap;
import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.dictionary.DictionaryPrivate;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.PFCDictionarySection;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.hdt.HDTManagerTest;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
import com.the_qa_company.qendpoint.core.util.concurrent.SyncSeq;
import com.the_qa_company.qendpoint.core.util.io.AbstractMapMemoryTest;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import org.apache.commons.io.file.PathUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Suite.SuiteClasses({ KCatMergerTest.BaseTest.class, KCatMergerTest.QuadCatDiffTests.class })
@RunWith(Suite.class)
public class KCatMergerTest extends AbstractMapMemoryTest {
	@RunWith(Parameterized.class)
	public static class BaseTest {

		@Parameterized.Parameters(name = "multi: {0}, unicode: {1}, map: {2}, count: {3}")
		public static Collection<Object[]> params() {
			return Stream.of(false, true)
					.flatMap(multi -> Stream.of(false, true)
							.flatMap(unicode -> Stream.of(false, true).flatMap(
									map -> Stream.of(2, 10).map(kcat -> new Object[] { multi, unicode, map, kcat }))))
					.collect(Collectors.toList());
		}

		@Parameterized.Parameter
		public boolean multi;
		@Parameterized.Parameter(1)
		public boolean unicode;
		@Parameterized.Parameter(2)
		public boolean map;
		@Parameterized.Parameter(3)
		public int kcat;

		@Rule
		public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

		private void writeSection(DictionarySection sec, OutputStream stream) throws IOException {
			((DictionarySectionPrivate) sec).save(stream, null);
		}

		private DictionarySection loadSection(InputStream stream) throws IOException {
			PFCDictionarySection section = new PFCDictionarySection(HDTOptions.EMPTY);
			section.load(stream, null);
			return section;
		}

		private Map<? extends CharSequence, DictionarySection> loadMultiSection(List<CharSequence> seq,
				InputStream stream) throws IOException {
			Map<ByteString, DictionarySection> sectionMap = new TreeMap<>();
			for (CharSequence key : seq) {
				PFCDictionarySection section = new PFCDictionarySection(HDTOptions.EMPTY);
				section.load(stream, null);
				sectionMap.put(ByteString.of(key), section);
			}
			return sectionMap;
		}

		@Test
		public void mergerTest() throws ParserException, IOException, InterruptedException {
			Path root = tempDir.newFolder().toPath();
			try {
				HDTOptions spec = HDTOptions.of();

				if (multi) {
					spec.set(HDTOptionsKeys.DICTIONARY_TYPE_KEY, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS);
					spec.set(HDTOptionsKeys.TEMP_DICTIONARY_IMPL_KEY,
							HDTOptionsKeys.TEMP_DICTIONARY_IMPL_VALUE_MULT_HASH);
				}

				// create "kcat" fake HDTs
				LargeFakeDataSetStreamSupplier s = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(1000, 42)
						.withMaxElementSplit(50).withUnicode(unicode);

				List<String> hdts = new ArrayList<>();
				for (int i = 0; i < kcat; i++) {
					String location = root.resolve("hdt" + i + ".hdt").toAbsolutePath().toString();
					hdts.add(location);
					s.createAndSaveFakeHDT(spec, location);
				}

				// create the excepted HDT from previous algorithm
				Path fatcathdt = root.resolve("fatcat.hdt");
				LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(1000L * kcat, 42).withMaxElementSplit(50)
						.withUnicode(unicode).createAndSaveFakeHDT(spec, fatcathdt.toAbsolutePath().toString());

				// create dictionary and write sections
				Path dictFile = root.resolve("dict");
				List<CharSequence> sub = new ArrayList<>();
				try (KCatImpl impl = KCatImpl.of(hdts, spec, null)) {
					try (KCatMerger merger = impl.createMerger(null)) {
						assertEquals(multi, merger.typedHDT);
						merger.startMerger();
						// create
						DictionaryPrivate dict = merger.buildDictionary();
						try (OutputStream stream = new BufferedOutputStream(Files.newOutputStream(dictFile))) {
							writeSection(dict.getShared(), stream);
							writeSection(dict.getSubjects(), stream);
							writeSection(dict.getPredicates(), stream);
							if (multi) {
								for (Map.Entry<? extends CharSequence, DictionarySection> e : dict.getAllObjects()
										.entrySet()) {
									CharSequence key = e.getKey();
									sub.add(key);
									DictionarySection sec = e.getValue();
									writeSection(sec, stream);
								}
							} else {
								writeSection(dict.getObjects(), stream);
							}
						}

						// check if all the dynamic sequences are filled

						SyncSeq[] sms = merger.subjectsMaps;
						SyncSeq[] pms = merger.predicatesMaps;
						SyncSeq[] oms = merger.objectsMaps;

						AtomicLong[] objectCounts = merger.countObject;
						AtomicLong[] subjectCounts = merger.countSubject;

						for (int hdtId = 1; hdtId <= impl.hdts.length; hdtId++) {
							HDT hdt = impl.hdts[hdtId - 1];
							SyncSeq sm = sms[hdtId - 1];
							SyncSeq pm = pms[hdtId - 1];
							SyncSeq om = oms[hdtId - 1];

							AtomicLong objectCount = objectCounts[hdtId - 1];
							AtomicLong subjectCount = subjectCounts[hdtId - 1];

							long shared = hdt.getDictionary().getShared().getNumberOfElements();
							long subjects = hdt.getDictionary().getSubjects().getNumberOfElements();
							long predicates = hdt.getDictionary().getPredicates().getNumberOfElements();
							long objects = multi
									? hdt.getDictionary().getAllObjects().values().stream()
											.mapToLong(DictionarySection::getNumberOfElements).sum()
									: hdt.getDictionary().getObjects().getNumberOfElements();

							assertEquals(shared + objects, objectCount.get());
							assertEquals(shared + subjects, subjectCount.get());

							for (long i = 1; i <= shared; i++) {
								long sv = sm.get(i);
								long ov = om.get(i);
								if (merger.removeHeader(sv) == 0) {
									fail("HDT #" + hdtId + "/" + impl.hdts.length + " Missing shared subject #" + i
											+ "/" + shared + " for node: "
											+ hdt.getDictionary().idToString(i, TripleComponentRole.SUBJECT));
								}
								if (merger.removeHeader(ov) == 0) {
									fail("HDT #" + hdtId + "/" + impl.hdts.length + " Missing shared object #" + i + "/"
											+ shared + " for node: "
											+ hdt.getDictionary().idToString(i, TripleComponentRole.OBJECT));
								}

								assertEquals("shared element not mapped to the same object", ov, sv);
								assertTrue("shared mapped element isn't shared", merger.isShared(ov));
							}

							for (long i = 1; i <= subjects; i++) {
								if (merger.removeHeader(sm.get(shared + i)) == 0) {
									fail("HDT #" + hdtId + "/" + impl.hdts.length + " Missing subject #" + i + "/"
											+ subjects + " for node: "
											+ hdt.getDictionary().idToString(i + shared, TripleComponentRole.SUBJECT));
								}
							}

							for (long i = 1; i <= objects; i++) {
								if (merger.removeHeader(om.get(shared + i)) == 0) {
									fail("HDT #" + hdtId + "/" + impl.hdts.length + " Missing object #" + i + "/"
											+ subjects + " for node: "
											+ hdt.getDictionary().idToString(i + shared, TripleComponentRole.OBJECT));
								}
							}

							for (long i = 1; i <= predicates; i++) {
								if (pm.get(i) == 0) {
									fail("HDT #" + hdtId + "/" + impl.hdts.length + " Missing predicate #" + i + "/"
											+ subjects + " for node: "
											+ hdt.getDictionary().idToString(i, TripleComponentRole.PREDICATE));
								}
							}

						}
					}
				}
				try (InputStream stream = new BufferedInputStream(Files.newInputStream(dictFile))) {
					// read the sections
					try (DictionarySection sh = loadSection(stream);
							DictionarySection su = loadSection(stream);
							DictionarySection pr = loadSection(stream)) {
						Map<? extends CharSequence, DictionarySection> dictionarySectionMap;
						DictionarySection ob;
						if (multi) {
							ob = null;
							dictionarySectionMap = loadMultiSection(sub, stream);
						} else {
							dictionarySectionMap = Map.of();
							ob = loadSection(stream);
						}
						try {
							// map the excepted hdt
							try (HDT exceptedHDT = HDTManager.mapHDT(fatcathdt.toAbsolutePath().toString())) {
								Dictionary exceptedDict = exceptedHDT.getDictionary();
								assertNotEquals("Invalid test, shared section empty", 0,
										exceptedHDT.getDictionary().getShared().getNumberOfElements());
								// assert equals between the dictionaries
								HDTManagerTest.HDTManagerTestBase.assertEqualsHDT("Shared", exceptedDict.getShared(),
										sh);
								HDTManagerTest.HDTManagerTestBase.assertEqualsHDT("Subjects",
										exceptedDict.getSubjects(), su);
								HDTManagerTest.HDTManagerTestBase.assertEqualsHDT("Predicates",
										exceptedDict.getPredicates(), pr);
								if (multi) {
									Map<? extends CharSequence, DictionarySection> exceptedDictSub = exceptedDict
											.getAllObjects();
									dictionarySectionMap.forEach((key, sec) -> {
										DictionarySection subSec = exceptedDictSub.get(key);
										assertNotNull("sub#" + key + " wasn't found", subSec);
										HDTManagerTest.HDTManagerTestBase.assertEqualsHDT("Section#" + key, subSec,
												sec);
									});
								} else {
									assert ob != null;
									HDTManagerTest.HDTManagerTestBase.assertEqualsHDT("Objects",
											exceptedDict.getObjects(), ob);
								}
							}
						} finally {
							Closer.of(ob).with(dictionarySectionMap.values()).close();
						}
					}
				}
			} finally {
				PathUtils.deleteDirectory(root);
			}
		}

		@Test
		public void catTest() throws ParserException, IOException, NotFoundException {
			Path root = tempDir.newFolder().toPath();
			try {
				// number of HDTs
				int countPerHDT = 1000;
				Random rnd = new Random(58);

				// create the config
				HDTOptions spec = HDTOptions.of();
				if (multi) {
					spec.set(HDTOptionsKeys.DICTIONARY_TYPE_KEY, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS);
					spec.set(HDTOptionsKeys.TEMP_DICTIONARY_IMPL_KEY,
							HDTOptionsKeys.TEMP_DICTIONARY_IMPL_VALUE_MULT_HASH);
				}

				if (map) {
					spec.set(HDTOptionsKeys.HDTCAT_FUTURE_LOCATION, root.resolve("futurehc.hdt").toAbsolutePath());
				}

				// create "kcat" fake HDTs
				LargeFakeDataSetStreamSupplier s = LargeFakeDataSetStreamSupplier.createInfinite(42)
						.withMaxElementSplit(50).withUnicode(unicode);

				long size = 0;
				List<String> hdts = new ArrayList<>();
				for (int i = 0; i < kcat; i++) {
					String location = root.resolve("hdt" + i + ".hdt").toAbsolutePath().toString();
					hdts.add(location);
					int hdtSize = countPerHDT / 2 + rnd.nextInt(countPerHDT);
					size += hdtSize;
					s.withMaxTriples(hdtSize).createAndSaveFakeHDT(spec, location);
				}

				// create the excepted HDT from previous algorithm
				Path fatcathdt = root.resolve("fatcat.hdt");
				s.reset();
				s.withMaxTriples(size).createAndSaveFakeHDT(spec, fatcathdt.toAbsolutePath().toString());

				// create dictionary and write sections
				// map the excepted hdt
				try (HDT actualHDT = HDTManager.catHDT(hdts, spec, null)) {
					try (HDT exceptedHDT = HDTManager.mapHDT(fatcathdt.toAbsolutePath().toString())) {
						// assert equals between the dictionaries
						assertNotEquals(0, actualHDT.getDictionary().getShared().getNumberOfElements());
						HDTManagerTest.HDTManagerTestBase.assertEqualsHDT(exceptedHDT, actualHDT);
					}
				}
			} finally {
				PathUtils.deleteDirectory(root);
			}
		}

		@Test
		@Ignore
		public void catDiffTest() throws ParserException, IOException, NotFoundException {
			Path root = tempDir.newFolder().toPath();
			try {
				// number of HDTs
				int countPerHDT = 1000;
				Random rnd = new Random(58);

				// create the config
				HDTOptions spec = HDTOptions.of();
				if (multi) {
					spec.set(HDTOptionsKeys.DICTIONARY_TYPE_KEY, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS);
					spec.set(HDTOptionsKeys.TEMP_DICTIONARY_IMPL_KEY,
							HDTOptionsKeys.TEMP_DICTIONARY_IMPL_VALUE_MULT_HASH);
				}

				if (map) {
					spec.set(HDTOptionsKeys.HDTCAT_FUTURE_LOCATION, root.resolve("futurehc.hdt").toAbsolutePath());
				}

				// create "kcat" fake HDTs
				LargeFakeDataSetStreamSupplier s = LargeFakeDataSetStreamSupplier.createInfinite(42)
						.withMaxElementSplit(50).withUnicode(unicode);

				Random rndDelete = new Random(45678);
				List<String> hdts = new ArrayList<>();
				List<String> hdtsDiff = new ArrayList<>();
				List<Bitmap> deleteBitmaps = new ArrayList<>();
				String diffwork = root.resolve("diffwork").toAbsolutePath().toString();
				for (int i = 0; i < kcat; i++) {
					String location = root.resolve("hdt" + i + ".hdt").toAbsolutePath().toString();
					String locationPreDiff = root.resolve("hdt" + i + "pre.hdt").toAbsolutePath().toString();
					hdtsDiff.add(location);
					hdts.add(locationPreDiff);
					int hdtSize = countPerHDT / 2 + rnd.nextInt(countPerHDT);
					s.withMaxTriples(hdtSize).createAndSaveFakeHDT(spec, locationPreDiff);

					ModifiableBitmap bitmap = BitmapFactory.createRWBitmap(hdtSize + 1);
					deleteBitmaps.add(bitmap);

					int toDelete = rndDelete.nextInt(hdtSize);
					for (int j = 0; j < toDelete; j++) {
						int index = rndDelete.nextInt(hdtSize) + 1;
						bitmap.set(index, true);
					}

					try (HDT diffHDTBit = HDTManager.diffHDTBit(diffwork, locationPreDiff, bitmap, spec, null)) {
						diffHDTBit.saveToHDT(location, null);
					}
				}

				// create the excepted HDT from previous algorithm
				Path fatcathdt = root.resolve("fatcat.hdt");
				try (HDT hdt = HDTManager.catHDT(hdtsDiff, spec, null)) {
					hdt.saveToHDT(fatcathdt.toAbsolutePath().toString(), null);
				}

				// create dictionary and write sections
				// map the excepted hdt
				try (HDT actualHDT = HDTManager.diffBitCatHDT(hdts, deleteBitmaps, spec, null)) {
					try (HDT exceptedHDT = HDTManager.mapHDT(fatcathdt.toAbsolutePath().toString())) {
						// assert equals between the dictionaries
						assertNotEquals(0, actualHDT.getDictionary().getShared().getNumberOfElements());
						HDTManagerTest.HDTManagerTestBase.assertEqualsHDT(exceptedHDT, actualHDT);
					}
				}
			} finally {
				PathUtils.deleteDirectory(root);
			}
		}

		@Test
		public void diffMergerTest() throws ParserException, IOException, InterruptedException {
			Path root = tempDir.newFolder().toPath();
			try {
				// number of HDTs
				int countPerHDT = 1000;
				Random rnd = new Random(58);

				// create the config
				HDTOptions spec = HDTOptions.of();
				if (multi) {
					spec.set(HDTOptionsKeys.DICTIONARY_TYPE_KEY, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS);
					spec.set(HDTOptionsKeys.TEMP_DICTIONARY_IMPL_KEY,
							HDTOptionsKeys.TEMP_DICTIONARY_IMPL_VALUE_MULT_HASH);
				}

				if (map) {
					spec.set(HDTOptionsKeys.HDTCAT_FUTURE_LOCATION, root.resolve("futurehc.hdt").toAbsolutePath());
				}

				// create "kcat" fake HDTs
				LargeFakeDataSetStreamSupplier s = LargeFakeDataSetStreamSupplier.createInfinite(42)
						.withMaxElementSplit(50).withUnicode(unicode);

				Random rndDelete = new Random(45678);
				List<String> hdts = new ArrayList<>();
				List<String> hdtsDiff = new ArrayList<>();
				List<Bitmap> deleteBitmaps = new ArrayList<>();
				String diffwork = root.resolve("diffwork").toAbsolutePath().toString();
				for (int i = 0; i < kcat; i++) {
					String location = root.resolve("hdt" + i + ".hdt").toAbsolutePath().toString();
					String locationPreDiff = root.resolve("hdt" + i + "pre.hdt").toAbsolutePath().toString();
					hdtsDiff.add(location);
					hdts.add(locationPreDiff);
					int hdtSize = countPerHDT / 2 + rnd.nextInt(countPerHDT);
					s.withMaxTriples(hdtSize).createAndSaveFakeHDT(spec, locationPreDiff);

					ModifiableBitmap bitmap = BitmapFactory.createRWBitmap(hdtSize + 1);
					deleteBitmaps.add(bitmap);

					int toDelete = rndDelete.nextInt(hdtSize);
					for (int j = 0; j < toDelete; j++) {
						int index = rndDelete.nextInt(hdtSize) + 1;
						bitmap.set(index, true);
					}

					try (HDT diffHDTBit = HDTManager.diffHDTBit(diffwork, locationPreDiff, bitmap, spec, null)) {
						diffHDTBit.saveToHDT(location, null);
					}
				}

				// create the excepted HDT from previous algorithm
				Path fatcathdt = root.resolve("fatcat.hdt");
				try (HDT hdt = HDTManager.catHDT(hdtsDiff, spec, null)) {
					hdt.saveToHDT(fatcathdt.toAbsolutePath().toString(), null);
				}

				// create dictionary and write sections
				Path dictFile = root.resolve("dict");
				List<CharSequence> sub = new ArrayList<>();
				try (KCatImpl impl = KCatImpl.of(hdts, deleteBitmaps, spec, null)) {
					try (KCatMerger merger = impl.createMerger(null)) {
						assertEquals(multi, merger.typedHDT);
						merger.startMerger();
						// create
						DictionaryPrivate dict = merger.buildDictionary();
						try (OutputStream stream = new BufferedOutputStream(Files.newOutputStream(dictFile))) {
							writeSection(dict.getShared(), stream);
							writeSection(dict.getSubjects(), stream);
							writeSection(dict.getPredicates(), stream);
							if (multi) {
								for (Map.Entry<? extends CharSequence, DictionarySection> e : dict.getAllObjects()
										.entrySet()) {
									CharSequence key = e.getKey();
									sub.add(key);
									DictionarySection sec = e.getValue();
									writeSection(sec, stream);
								}
							} else {
								writeSection(dict.getObjects(), stream);
							}
						}
					}
				}
				try (InputStream stream = new BufferedInputStream(Files.newInputStream(dictFile))) {
					// read the sections
					try (DictionarySection sh = loadSection(stream);
							DictionarySection su = loadSection(stream);
							DictionarySection pr = loadSection(stream)) {
						Map<? extends CharSequence, DictionarySection> dictionarySectionMap;
						DictionarySection ob;
						if (multi) {
							ob = null;
							dictionarySectionMap = loadMultiSection(sub, stream);
						} else {
							dictionarySectionMap = Map.of();
							ob = loadSection(stream);
						}
						try {
							// map the excepted hdt
							try (HDT exceptedHDT = HDTManager.mapHDT(fatcathdt.toAbsolutePath().toString())) {
								Dictionary exceptedDict = exceptedHDT.getDictionary();
								assertNotEquals("Invalid test, shared section empty", 0,
										exceptedHDT.getDictionary().getShared().getNumberOfElements());
								// assert equals between the dictionaries
								HDTManagerTest.HDTManagerTestBase.assertEqualsHDT("Shared", exceptedDict.getShared(),
										sh);
								HDTManagerTest.HDTManagerTestBase.assertEqualsHDT("Subjects",
										exceptedDict.getSubjects(), su);
								HDTManagerTest.HDTManagerTestBase.assertEqualsHDT("Predicates",
										exceptedDict.getPredicates(), pr);
								if (multi) {
									Map<? extends CharSequence, DictionarySection> exceptedDictSub = exceptedDict
											.getAllObjects();
									dictionarySectionMap.forEach((key, sec) -> {
										DictionarySection subSec = exceptedDictSub.get(key);
										assertNotNull("sub#" + key + " wasn't found", subSec);
										HDTManagerTest.HDTManagerTestBase.assertEqualsHDT("Section#" + key, subSec,
												sec);
									});
								} else {
									assert ob != null;
									HDTManagerTest.HDTManagerTestBase.assertEqualsHDT("Objects",
											exceptedDict.getObjects(), ob);
								}
							}
						} finally {
							Closer.of(ob).with(dictionarySectionMap.values()).close();
						}
					}
				}
			} finally {
				PathUtils.deleteDirectory(root);
			}
		}
	}

	@RunWith(Parameterized.class)
	public static class QuadCatDiffTests extends AbstractMapMemoryTest {
		@Rule
		public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

		@Parameterized.Parameters(name = "{0} default:{1}")
		public static Collection<Object[]> params() {
			return Stream
					.of(HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_QUAD_SECTION,
							HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG_QUAD)
					.flatMap(dict -> Stream.of(true, false).map(defaultGraph -> new Object[] { dict, defaultGraph }))
					.toList();
		}

		@Parameterized.Parameter
		public String dictType;
		@Parameterized.Parameter(1)
		public boolean defaultGraph;

		public HDTOptions spec;
		public Path root;

		@Before
		public void setupVals() throws IOException {
			spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY, dictType);
			root = tempDir.newFolder().toPath();
		}

		@After
		public void clearVals() throws IOException {
			PathUtils.deleteDirectory(root);
		}

		@Test
		public void catTest() throws ParserException, IOException, NotFoundException {
			long size = 1000;
			int count = 10;

			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxTriples(size, 6789).withMaxElementSplit((int) (size / 50)).withQuads(true)
					.withNoDefaultGraph(!defaultGraph);

			List<Path> files = IntStream.range(0, count).mapToObj(i -> root.resolve("sub-" + i + ".hdt")).toList();

			List<Bitmap> deletes = new ArrayList<>();
			// create the files to cat
			for (Path file : files) {
				try (HDT hdt = supplier.createFakeHDT(spec)) {
					hdt.saveToHDT(file);
					deletes.add(GraphDeleteBitmap.empty(hdt.getDictionary().getNgraphs(),
							hdt.getTriples().getNumberOfElements()));
				}
			}

			supplier.withMaxTriples(count * size);
			supplier.reset();

			Path exceptedHDT = root.resolve("excepted.hdt");

			supplier.createAndSaveFakeHDT(spec, exceptedHDT);

			Path actualHDT = root.resolve("actual.hdt");
			Path actualdiffHDT = root.resolve("actualdiff.hdt");

			// compute using cat algorithm
			try (HDT hdt = HDTManager.catHDTPath(files, spec, ProgressListener.ignore())) {
				HDTManagerTest.HDTManagerTestBase.checkHDTConsistency(hdt);
				assertTrue(hdt.getDictionary().supportGraphs());
				hdt.saveToHDT(actualHDT);
			}

			// compute using diff algorithm with empty bitmaps
			try (HDT hdt = HDTManager.diffBitCatHDTPath(files, deletes, spec, ProgressListener.ignore())) {
				HDTManagerTest.HDTManagerTestBase.checkHDTConsistency(hdt);
				assertTrue(hdt.getDictionary().supportGraphs());
				hdt.saveToHDT(actualdiffHDT);
			}

			// test results
			try (HDT excepted = HDTManager.mapHDT(exceptedHDT);
					HDT actual = HDTManager.mapHDT(actualHDT);
					HDT actualdiff = HDTManager.mapHDT(actualdiffHDT)) {
				// clear original size because it impossible to compute it in a
				// diff
				excepted.getHeader().remove("_:statistics", HDTVocabulary.ORIGINAL_SIZE, "");
				actual.getHeader().remove("_:statistics", HDTVocabulary.ORIGINAL_SIZE, "");
				actualdiff.getHeader().remove("_:statistics", HDTVocabulary.ORIGINAL_SIZE, "");

				HDTManagerTest.HDTManagerTestBase.checkHDTConsistency(actual);
				HDTManagerTest.HDTManagerTestBase.assertEqualsHDT(excepted, actual);
				HDTManagerTest.HDTManagerTestBase.checkHDTConsistency(actualdiff);
				HDTManagerTest.HDTManagerTestBase.assertEqualsHDT(excepted, actualdiff);
			}
		}

		@Test
		public void catDiffTest() throws ParserException, IOException, NotFoundException {
			long size = 1625;
			int count = 10;

			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxTriples(size, 6789).withMaxElementSplit((int) (size / 50)).withQuads(true)
					.withNoDefaultGraph(!defaultGraph);

			List<Path> files = IntStream.range(0, count).mapToObj(i -> root.resolve("sub-" + i + ".hdt")).toList();

			List<Bitmap> deleteBitmaps = new ArrayList<>();
			Random deleteRnd = new Random(345678);
			try {

				List<TripleString> dataset = new ArrayList<>();

				// create the files to cat
				for (Path file : files) {
					try (HDT hdt = supplier.createFakeHDT(spec)) {
						hdt.saveToHDT(file);
						long graphs = hdt.getDictionary().supportGraphs() ? hdt.getDictionary().getNgraphs() : 1;
						assert graphs > 0;
						long triples = hdt.getTriples().getNumberOfElements();

						GraphDeleteBitmap memory = GraphDeleteBitmap.memory(graphs, triples + 1);

						// create delete bitmap
						int toDelete = (int) (triples / 200);
						for (int i = 0; i < toDelete; i++) {
							int tripleId = 1 + deleteRnd.nextInt((int) triples);

							// add +1 for a special case to delete all the nodes
							int graphId = deleteRnd.nextInt((int) graphs + 1);

							if (graphId >= graphs) {
								// delete the triple
								for (int j = 0; j < graphs; j++) {
									memory.set(j, tripleId, true);
								}
							} else {
								// delete the quad
								memory.set(graphId, tripleId, true);
							}
						}

						IteratorTripleString it = hdt.searchAll();

						while (it.hasNext()) {
							TripleString ts = it.next();
							long pos = it.getLastTriplePosition();
							long g = ts.getGraph().isEmpty() ? 0
									: (hdt.getDictionary().stringToId(ts.getGraph(), TripleComponentRole.GRAPH) - 1);

							assert g >= 0;

							if (!memory.access(g, pos)) {
								// not deleted, we can add it
								dataset.add(ts.tripleToString());
							}
						}

						deleteBitmaps.add(memory);
					}
				}

				supplier.withMaxTriples(count * size);

				Path exceptedHDT = root.resolve("excepted.hdt");

				try (HDT hdt = HDTManager.generateHDT(dataset.iterator(), LargeFakeDataSetStreamSupplier.BASE_URI, spec,
						ProgressListener.ignore())) {
					hdt.saveToHDT(exceptedHDT);
				}

				Path actualHDT = root.resolve("actual.hdt");
				try (HDT hdt = HDTManager.diffBitCatHDTPath(files, deleteBitmaps, spec, ProgressListener.ignore())) {
					HDTManagerTest.HDTManagerTestBase.checkHDTConsistency(hdt);
					hdt.saveToHDT(actualHDT);
				}

				try (HDT excepted = HDTManager.mapHDT(exceptedHDT); HDT actual = HDTManager.mapHDT(actualHDT)) {
					HDTManagerTest.HDTManagerTestBase.checkHDTConsistency(actual);

					// clear original size because it impossible to compute it
					// in a diff
					excepted.getHeader().remove("_:statistics", HDTVocabulary.ORIGINAL_SIZE, "");
					actual.getHeader().remove("_:statistics", HDTVocabulary.ORIGINAL_SIZE, "");
					HDTManagerTest.HDTManagerTestBase.assertEqualsHDT(excepted, actual);
				}
			} finally {
				Closer.closeAll(deleteBitmaps);
			}
		}
	}
}
