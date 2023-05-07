package com.the_qa_company.qendpoint.core.storage;

import com.the_qa_company.qendpoint.core.compact.bitmap.EmptyBitmap;
import com.the_qa_company.qendpoint.core.compact.bitmap.ModifiableBitmap;
import com.the_qa_company.qendpoint.core.compact.sequence.LargeArrayTest;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
import com.the_qa_company.qendpoint.core.util.disk.LongArray;
import org.apache.commons.io.file.PathUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

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
		QEPMap.endSync.registerAction(this::mapOrderTest);
	}

	@After
	public void complete() throws Exception {
		PathUtils.deleteDirectory(root);
		QEPMap.endSync.throwExceptionResult();
	}

	private void mapOrderTest(QEPMap qepMap) {
		QEPMap.SectionMap pmap = qepMap.maps[TripleComponentRole.PREDICATE.ordinal()];

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

			assertEquals(v2, pconv.dataset2to1().mapValue(v1));
			assertEquals(v1, pconv.dataset1to2().mapValue(v2));
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

					// TODO: test mapping


					lastId = id;
				}
			}
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

		try (QEPDataset dataset1 = new QEPDataset(null, "d1", d1, HDTManager.mapHDT(d1), EmptyBitmap.of(0),
				EMPTY_DELTA);
		     QEPDataset dataset2 = new QEPDataset(null, "d2", d2, HDTManager.mapHDT(d2), EmptyBitmap.of(0),
				     EMPTY_DELTA);
		     QEPMap map = new QEPMap(root.resolve("maps"), dataset1, dataset2)) {
			map.sync();
		}

		try (QEPDataset dataset1 = new QEPDataset(null, "d1", d1, HDTManager.mapHDT(d1), EmptyBitmap.of(0),
				EMPTY_DELTA);
		     QEPDataset dataset2 = new QEPDataset(null, "d2", d2, HDTManager.mapHDT(d2), EmptyBitmap.of(0),
				     EMPTY_DELTA);
		     QEPMap map = new QEPMap(root.resolve("maps"), dataset1, dataset2)) {
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

		try (QEPDataset dataset1 = new QEPDataset(null, "d1", d1, HDTManager.mapHDT(d1), EmptyBitmap.of(0),
				EMPTY_DELTA);
		     QEPDataset dataset2 = new QEPDataset(null, "d2", d2, HDTManager.mapHDT(d2), EmptyBitmap.of(0),
				     EMPTY_DELTA);
		     QEPMap map = new QEPMap(root.resolve("maps"), dataset1, dataset2)) {
			map.sync();
		}

		try (
				// use same id with different HDT to create an error
				QEPDataset dataset1 = new QEPDataset(null, "d1", d1, HDTManager.mapHDT(d3), EmptyBitmap.of(0),
						EMPTY_DELTA);
				QEPDataset dataset2 = new QEPDataset(null, "d2", d2, HDTManager.mapHDT(d4), EmptyBitmap.of(0),
						EMPTY_DELTA);
				QEPMap map = new QEPMap(root.resolve("maps"), dataset1, dataset2)) {
			map.sync();
		}
	}
}
