package com.the_qa_company.qendpoint.core.storage;

import com.the_qa_company.qendpoint.core.compact.bitmap.EmptyBitmap;
import com.the_qa_company.qendpoint.core.compact.bitmap.ModifiableBitmap;
import com.the_qa_company.qendpoint.core.compact.sequence.LargeArrayTest;
import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.storage.converter.NodeConverter;
import com.the_qa_company.qendpoint.core.storage.converter.PermutationNodeConverter;
import com.the_qa_company.qendpoint.core.storage.converter.SharedWrapperNodeConverter;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
import com.the_qa_company.qendpoint.core.util.disk.LongArray;
import org.apache.commons.io.file.PathUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.LongStream;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;

public class QEPMapTest {
	private static final ModifiableBitmap[] EMPTY_DELTA;

	static {
		EMPTY_DELTA = new ModifiableBitmap[TripleComponentRole.values().length];
		Arrays.fill(EMPTY_DELTA, EmptyBitmap.of(0));
	}

	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	Path root;

	@Before
	public void setup() throws IOException {
		root = tempDir.newFolder().toPath();
		QEPMap.endSync.registerAction(QEPMapTest::mapOrderTest);
	}

	@After
	public void complete() throws Exception {
		PathUtils.deleteDirectory(root);
		QEPMap.endSync.throwExceptionResult();
	}

	/**
	 * dump the map in the tests directory
	 *
	 * @param qepMap map
	 */
	@SuppressWarnings("unused")
	public static void dumpMap(String outputDir, QEPMap qepMap) {
		dumpMap(outputDir, qepMap, null);
	}
	/**
	 * dump the map in the tests directory
	 *
	 * @param qepMap map
	 */
	@SuppressWarnings("unused")
	public static void dumpMap(String outputDir, QEPMap qepMap, String errorMessage) {
		try {
			Path output = Path.of("tests").resolve(outputDir);
			{
				String prefix = TripleComponentRole.PREDICATE.getAbbreviation() + qepMap.uid.uid1() + "-" + qepMap.uid.uid2() + "-";
				QEPMap.SectionMap map = qepMap.maps[TripleComponentRole.PREDICATE.ordinal()];
				Path routput = output.resolve(TripleComponentRole.PREDICATE.getTitle());
				Files.createDirectories(routput);
				try (BufferedWriter w = Files.newBufferedWriter(routput.resolve(prefix + "d1.bin"))) {
					w.write("map (uid) " + qepMap.uid.uid1() + " -> " + qepMap.uid.uid2() + " (" + TripleComponentRole.PREDICATE.getTitle() + ")\n");
					long len = map.mapSequence1().length();
					for (long i = 0; i < len; i++) {
						w.write(format("%16X -> %16X\n", map.idSequence1().get(i), map.mapSequence1().get(i)));
						if (i % 100 == 0) {
							w.flush();
						}
					}
				}
				try (BufferedWriter w = Files.newBufferedWriter(routput.resolve(prefix + "d2.bin"))) {
					w.write("map (uid) " + qepMap.uid.uid1() + " -> " + qepMap.uid.uid2() + " (" + TripleComponentRole.PREDICATE.getTitle() + ")\n");
					long len = map.mapSequence2().length();
					for (long i = 0; i < len; i++) {
						w.write(format("%16X -> %16X\n", map.idSequence2().get(i), map.mapSequence2().get(i)));
						if (i % 100 == 0) {
							w.flush();
						}
					}
				}
			}

			for (TripleComponentRole role : new TripleComponentRole[]{
					TripleComponentRole.SUBJECT, TripleComponentRole.OBJECT
			}) {
				String prefix = role.getAbbreviation() + qepMap.uid.uid1() + "-" + qepMap.uid.uid2() + "-";
				QEPMap.SectionMap map = qepMap.maps[role.ordinal()];
				Path routput = output.resolve(role.getTitle());
				Files.createDirectories(routput);
				try (BufferedWriter w = Files.newBufferedWriter(routput.resolve(prefix + "d1.bin"))) {
					w.write("map (uid) " + qepMap.uid.uid1() + " -> " + qepMap.uid.uid2() + " (" + role.getTitle() + ")\n");
					long len = map.mapSequence1().length();
					for (long i = 0; i < len; i++) {
						long mappedId = map.mapSequence1().get(i);
						w.write(format("%16X -> %16X %s\n", map.idSequence1().get(i),
								mappedId >>> 1,
								((mappedId & 1) == 0 ? TripleComponentRole.SUBJECT : TripleComponentRole.OBJECT).getAbbreviation()
						));
						if (i % 100 == 0) {
							w.flush();
						}
					}
				}
				try (BufferedWriter w = Files.newBufferedWriter(routput.resolve(prefix + "d2.bin"))) {
					w.write("map (uid) " + qepMap.uid.uid1() + " -> " + qepMap.uid.uid2() + " (" + role.getTitle() + ")\n");
					long len = map.mapSequence2().length();
					for (long i = 0; i < len; i++) {
						long mappedId = map.mapSequence2().get(i);
						w.write(format("%16X -> %16X %s\n", map.idSequence2().get(i),
								mappedId >>> 1,
								((mappedId & 1) == 0 ? TripleComponentRole.SUBJECT : TripleComponentRole.OBJECT).getAbbreviation()
						));
						if (i % 100 == 0) {
							w.flush();
						}
					}
				}
			}
			if (errorMessage != null) {
				throw new AssertionError(errorMessage + " (map dumped: " + output.toAbsolutePath() + ")");
			}
		} catch (IOException e) {
			if (errorMessage != null) {
				throw new AssertionError(errorMessage, e);
			}
		}
	}

	private static void mapOrderTest(QEPMap qepMap) {
		QEPMap.SectionMap pmap = qepMap.maps[TripleComponentRole.PREDICATE.ordinal()];
		Uid uid = qepMap.getUid();
		QEPDataset d1 = qepMap.getDataset(uid.uid1());
		Dictionary dict1 = d1.dataset().getDictionary();
		QEPDataset d2 = qepMap.getDataset(uid.uid2());
		Dictionary dict2 = d2.dataset().getDictionary();


		// using the same map for both plinks
		assertSame(pmap.idSequence1(), pmap.mapSequence2());
		assertSame(pmap.mapSequence1(), pmap.idSequence2());
		// assert that the ids are sorted on both even if we only read seq the id1
		// test mapping pid->pid
		LargeArrayTest.assertSorted(pmap.idSequence1(), false);
		LargeArrayTest.assertSorted(pmap.mapSequence1(), false);

		// test predicate mapping
		QEPMap.DatasetNodeConverter pconv = qepMap.nodeConverters[TripleComponentRole.PREDICATE.ordinal()];

		for (long i = 1; i < pmap.idSequence1().length(); i++) {
			long v1 = pmap.idSequence1().get(i);
			long v2 = pmap.idSequence2().get(i);

			CharSequence str1 = dict1.idToString(v2, TripleComponentRole.PREDICATE);
			CharSequence str2 = dict2.idToString(v1, TripleComponentRole.PREDICATE);

			assertEquals(v2, pconv.dataset2to1().mapValue(v1));
			assertEquals(v1, pconv.dataset1to2().mapValue(v2));
			assertEquals("bad mapped strings", str1, str2);
		}

		// test mapping so->so
		for (TripleComponentRole role : new TripleComponentRole[]{
				TripleComponentRole.SUBJECT,
				TripleComponentRole.OBJECT
		}) {
			int roleId = role.ordinal();
			QEPMap.SectionMap map = qepMap.maps[roleId];

			LargeArrayTest.assertSorted(map.idSequence1(), false);
			LargeArrayTest.assertSorted(map.idSequence2(), false);

			for (int seqId = 0; seqId < 2; seqId++) {
				LongArray idSequence = map.idByNumber(seqId);
				LongArray mapSequence = map.mapByNumber(seqId);

				assertEquals(format("bad sequence length map#%d", seqId + 1), idSequence.length(),
						mapSequence.length());

				// converter using the 2 maps
				PermutationNodeConverter converter = new PermutationNodeConverter(idSequence, mapSequence);


				long lastId = 0;
				for (int i = 1; i < idSequence.length(); i++) {
					long id = idSequence.get(i);

					if (id <= lastId) {
						StringBuilder s = new StringBuilder(format("Bad order IDS%d/%s, [%d/%d]: %d >= %d\n", seqId,
								role, i, idSequence.length() - 1, id, lastId));

						int startIndex = Math.max(1, i - 10);
						long endIndex = Math.min(idSequence.length(), i + 10);
						for (int j = startIndex; j < endIndex; j++) {
							s.append(format("%d/%d ", j, idSequence.get(j)));
						}

						throw new AssertionError(s.toString());
					}
					long bsid = idSequence.binarySearch(id);
					assertNotEquals(bsid, -1);
					if (i != bsid) {
						throw new AssertionError(format("bad bsid: %d != %d", i, bsid));
					}

					long mapped = mapSequence.get(i);

					assertEquals("bad converter mapping", mapped, converter.mapValue(id));

					// SUBJECT/OBJECT
					TripleComponentRole roleOther = QEPMap.getRoleOfMapped(mapped);
					QEPMap.SectionMap mapOther = qepMap.maps[roleOther.ordinal()];
					// fetching the other maps
					LongArray idSequenceOther = mapOther.idByNumber(1 ^ seqId);
					LongArray mapSequenceOther = mapOther.mapByNumber(1 ^ seqId);

					long nshared = map.nsByNumber(seqId);
					long nsharedOther = mapOther.nsByNumber(seqId ^ 1);

					long mappedId = QEPMap.getIdOfMapped(mapped, nsharedOther);

					long searchedId = mappedId - (roleOther == TripleComponentRole.OBJECT ? nsharedOther : 0);
					long mbsid = idSequenceOther.binarySearch(searchedId);
					assertNotEquals("bad mbsid", mbsid, -1);
					assertEquals("bad binary search", idSequenceOther.get(mbsid), searchedId);

					long mappedMapped = mapSequenceOther.get(mbsid);
					assertEquals("remapped role isn't the same", role, QEPMap.getRoleOfMapped(mappedMapped));
					long idOfMapped = QEPMap.getIdOfMapped(mappedMapped, nshared);
					if (id != idOfMapped - (role == TripleComponentRole.OBJECT ? nshared : 0)) {
						StringBuilder s = new StringBuilder(
								"""
										[%s->%s]
										f^-1(f(x)) isn't x *[mappedMapped=%d]= idOfMapped=%d != id=%d
										for mbsid=%d/searchedId=%d
										shared=%d/sharedOther=%d
										""".formatted(role, roleOther, mappedMapped, idOfMapped, id,
										mbsid, searchedId,
										nshared, nsharedOther));

						for (long j = Math.max(1, mbsid - 10); j < Math.min(mapSequenceOther.length(),
								mbsid + 10); j++) {
							long aid = mapSequenceOther.get(j);
							long aimap = idSequenceOther.get(j);
							s.append(format("%d/%d/%s:%d ", j, aimap,
									QEPMap.getRoleOfMapped(aid).getAbbreviation(),
									QEPMap.getIdOfMapped(aid, nshared)));
							if (j % 5 == 0) {
								s.append("\n");
							}
						}
						s.append("\n");

						throw new AssertionError(s.toString());
					}

					lastId = id;
				}
			}

			NodeConverter c12 = qepMap.getConverter(uid.uid1(), uid.uid2(), role);
			NodeConverter c21 = qepMap.getConverter(uid.uid2(), uid.uid1(), role);

			Iterator<QEPMapIdSorter.QEPMapIds> it12 = getIdsIterator(c12);
			long shared12 = getSharedCount(c12);

			long i = 0;
			while (it12.hasNext()) {
				QEPMapIdSorter.QEPMapIds ids = it12.next();
				long mappedValue = c12.mapValue(ids.origin());
				if (ids.destination() != mappedValue) {
					dumpMap("maptest", qepMap, format("destination=%X != mappedValue=%X\n[%X] %X/%X %X %s %s",
							ids.destination(), mappedValue, i, ids.origin(), shared12, ids.origin() - shared12, role, c12));
				}

				CharSequence str1 = dict1.idToString(ids.origin(), role);
				CharSequence str2 = dict2.idToString(
						QEPMap.getIdOfMapped(mappedValue, dict2.getNshared()),
						QEPMap.getRoleOfMapped(mappedValue)
				);

				assertEquals("bad mapped id for " + role, str1, str2);
				i++;
			}

			Iterator<QEPMapIdSorter.QEPMapIds> it21 = getIdsIterator(c21);
			long shared21 = getSharedCount(c12);

			i = 0;
			while (it21.hasNext()) {
				QEPMapIdSorter.QEPMapIds ids = it21.next();
				long mappedValue = c21.mapValue(ids.origin());
				if (ids.destination() != mappedValue) {
					dumpMap("maptest", qepMap, format("destination=%X != mappedValue=%x\n[%x] %X/%X %X %s %s",
							ids.destination(), mappedValue, i, ids.origin(), shared21, ids.origin() - shared21, role, c12));
				}

				CharSequence str1 = dict1.idToString(
						QEPMap.getIdOfMapped(mappedValue, dict1.getNshared()),
						QEPMap.getRoleOfMapped(mappedValue)
				);
				CharSequence str2 = dict2.idToString(
						ids.origin(), role
				);

				assertEquals("bad mapped id for " + role, str1, str2);
				i++;
			}

		}
	}

	private static long getSharedCount(NodeConverter converter) {
		if (converter instanceof SharedWrapperNodeConverter shared) {
			return shared.sharedCount();
		}
		return 0;
	}
	private static Iterator<QEPMapIdSorter.QEPMapIds> getIdsIterator(NodeConverter converter) {
		if (converter instanceof PermutationNodeConverter perm) {
			LongArray ids = perm.idSequence();
			LongArray maps = perm.mapSequence();
			return LongStream
					.range(1, ids.length())
					.mapToObj(id -> new QEPMapIdSorter.QEPMapIds(ids.get(id), maps.get(id)))
					.iterator();
		} else if (converter instanceof SharedWrapperNodeConverter shared) {
			if (!(
					shared.subjectConverter() instanceof PermutationNodeConverter subjectConverter
							&& shared.objectConverter() instanceof PermutationNodeConverter objectConverter
			)) {
				throw new AssertionError("bad permutation type");
			}
			LongArray sids = subjectConverter.idSequence();
			LongArray smaps = subjectConverter.mapSequence();
			LongArray oids = objectConverter.idSequence();
			LongArray omaps = objectConverter.mapSequence();

			long sharedCount = shared.sharedCount();
			long sharedSwapLocation = sids.binarySearchLocation(sharedCount);

			return LongStream
					.range(1, sharedSwapLocation + oids.length() - 1)
					.mapToObj(id -> {
						if (id > sharedSwapLocation) {
							return new QEPMapIdSorter.QEPMapIds(oids.get(id - sharedSwapLocation) + sharedCount, omaps.get(id - sharedSwapLocation));
						}

						return new QEPMapIdSorter.QEPMapIds(sids.get(id), smaps.get(id));
					})
					.iterator();
		} else {
			throw new AssertionError("bad node converter type: " + converter.getClass());
		}
	}

	@Test
	public void linkReloadTest() throws IOException, ParserException {
		LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
				.createSupplierWithMaxTriples(10_000, 34).withMaxElementSplit(50).withMaxLiteralSize(100)
				.withUnicode(true);

		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS, HDTOptionsKeys.LOADER_TYPE_KEY,
				HDTOptionsKeys.LOADER_TYPE_VALUE_DISK, HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, root.resolve("gen"));
		Path store = root.resolveSibling("store");

		Files.createDirectories(store);

		Path d1 = store.resolve("d1.hdt");
		Path d2 = store.resolve("d2.hdt");
		supplier.createAndSaveFakeHDT(spec, d1);
		supplier.createAndSaveFakeHDT(spec, d2);

		try (QEPDataset dataset1 = new QEPDataset(QEPCoreTest.EMPTY_CORE, "d1", d1, HDTManager.mapHDT(d1), EmptyBitmap.of(0),
				EMPTY_DELTA);
		     QEPDataset dataset2 = new QEPDataset(QEPCoreTest.EMPTY_CORE, "d2", d2, HDTManager.mapHDT(d2), EmptyBitmap.of(0),
				     EMPTY_DELTA);
		     QEPMap map = new QEPMap(root.resolve("maps"), QEPCoreTest.EMPTY_CORE, dataset1, dataset2)) {
			map.sync();
		}

		try (QEPDataset dataset1 = new QEPDataset(QEPCoreTest.EMPTY_CORE, "d1", d1, HDTManager.mapHDT(d1), EmptyBitmap.of(0),
				EMPTY_DELTA);
		     QEPDataset dataset2 = new QEPDataset(QEPCoreTest.EMPTY_CORE, "d2", d2, HDTManager.mapHDT(d2), EmptyBitmap.of(0),
				     EMPTY_DELTA);
		     QEPMap map = new QEPMap(root.resolve("maps"), QEPCoreTest.EMPTY_CORE, dataset1, dataset2)) {
			map.sync();
		}
	}

	@Test(expected = IOException.class)
	public void linkReloadErrTest() throws IOException, ParserException {
		LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
				.createSupplierWithMaxTriples(10_000, 34).withMaxElementSplit(50).withMaxLiteralSize(100)
				.withUnicode(true);

		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS, HDTOptionsKeys.LOADER_TYPE_KEY,
				HDTOptionsKeys.LOADER_TYPE_VALUE_DISK, HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, root.resolve("gen"));
		Path store = root.resolve("store");

		Files.createDirectories(store);

		Path d1 = store.resolve("d1.hdt");
		Path d2 = store.resolve("d2.hdt");
		Path d3 = store.resolve("d3.hdt");
		Path d4 = store.resolve("d4.hdt");
		supplier.createAndSaveFakeHDT(spec, d1);
		supplier.createAndSaveFakeHDT(spec, d2);
		supplier.createAndSaveFakeHDT(spec, d3);
		supplier.createAndSaveFakeHDT(spec, d4);

		try (QEPDataset dataset1 = new QEPDataset(QEPCoreTest.EMPTY_CORE, "d1", d1, HDTManager.mapHDT(d1), EmptyBitmap.of(0),
				EMPTY_DELTA);
		     QEPDataset dataset2 = new QEPDataset(QEPCoreTest.EMPTY_CORE, "d2", d2, HDTManager.mapHDT(d2), EmptyBitmap.of(0),
				     EMPTY_DELTA);
		     QEPMap map = new QEPMap(root.resolve("maps"), QEPCoreTest.EMPTY_CORE, dataset1, dataset2)) {
			map.sync();
		}

		try (
				// use same id with different HDT to create an error
				QEPDataset dataset1 = new QEPDataset(QEPCoreTest.EMPTY_CORE, "d1", d1, HDTManager.mapHDT(d3), EmptyBitmap.of(0),
						EMPTY_DELTA);
				QEPDataset dataset2 = new QEPDataset(QEPCoreTest.EMPTY_CORE, "d2", d2, HDTManager.mapHDT(d4), EmptyBitmap.of(0),
						EMPTY_DELTA);
				QEPMap map = new QEPMap(root.resolve("maps"), QEPCoreTest.EMPTY_CORE, dataset1, dataset2)) {
			map.sync();
		}
	}
}
