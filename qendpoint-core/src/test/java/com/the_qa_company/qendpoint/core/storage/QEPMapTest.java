package com.the_qa_company.qendpoint.core.storage;

import com.the_qa_company.qendpoint.core.compact.bitmap.EmptyBitmap;
import com.the_qa_company.qendpoint.core.compact.bitmap.ModifiableBitmap;
import com.the_qa_company.qendpoint.core.compact.sequence.DynamicSequence;
import com.the_qa_company.qendpoint.core.compact.sequence.Sequence;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
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

public class QEPMapTest {
	private static final ModifiableBitmap[] EMPTY_DELTA;

	static {
		EMPTY_DELTA = new ModifiableBitmap[TripleComponentRole.values().length];
		Arrays.fill(EMPTY_DELTA,EmptyBitmap.of(0));
	}
	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	Path root;

	@Before
	public void setup() throws IOException {
		root = tempDir.getRoot().toPath();
		QEPMap.endSync.registerAction(this::mapOrderTest);
	}

	@After
	public void complete() throws Exception {
		PathUtils.deleteDirectory(root);
		QEPMap.endSync.throwExceptionResult();
	}

	@SuppressWarnings("resource")
	private void mapOrderTest(QEPMap qepMap) {
		for (TripleComponentRole role : TripleComponentRole.values()) {
			int roleId = role.ordinal();
			QEPMap.SectionMap map = qepMap.maps[roleId];

			for (int seqId = 0; seqId < 2; seqId++) {
				QEPDataset current;
				QEPDataset other;
				if (qepMap.isMapDataset1Smaller() == (seqId == 0)) {
					current = qepMap.dataset1;
					other = qepMap.dataset2;
				} else {
					current = qepMap.dataset2;
					other = qepMap.dataset1;
				}

				DynamicSequence idSequence = map.idByNumber(seqId);
				DynamicSequence mapSequence = map.mapByNumber(seqId);

				assertEquals(format("bad sequence length map#%s", seqId+1), idSequence.length(), mapSequence.length());

				long lastId = 0;
				for (int i = 1; i < idSequence.length(); i++) {
					long id = idSequence.get(i);

					if (id <= lastId) {
						StringBuilder s = new StringBuilder(format("Bad order IDS%d/%s, [%d/%d]: %d >= %d\n",
								seqId, role, i, idSequence.length() - 1, id, lastId));

						for (int j = Math.max(1, i - 10); j < Math.min(idSequence.length(), i + 10); j++) {
							s.append(format("%d/%d ", j, idSequence.get(j)));
						}

						throw new AssertionError(s.toString());
					}
					long bsid = idSequence.binarySearch(id);
					assertNotEquals(bsid, -1);
					if (i != bsid) {
						throw new AssertionError(format("bad bsid: %d != %d", i, bsid));
					}
					lastId = id;
				}
			}
		}
	}

	@Test
	public void linkReloadTest() throws IOException, ParserException {
		LargeFakeDataSetStreamSupplier supplier
				= LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(10_000, 34)
				.withMaxElementSplit(50)
				.withMaxLiteralSize(100)
				.withUnicode(true);


		HDTOptions spec = HDTOptions.of(
				HDTOptionsKeys.DICTIONARY_TYPE_KEY, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS,
				HDTOptionsKeys.LOADER_TYPE_KEY, HDTOptionsKeys.LOADER_TYPE_VALUE_DISK,
				HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, root.resolve("gen")
		);
		Path store = root.resolveSibling("store");

		Files.createDirectories(store);

		Path d1 = store.resolve("d1.hdt");
		Path d2 = store.resolve("d2.hdt");
		supplier.createAndSaveFakeHDT(spec, d1);
		supplier.createAndSaveFakeHDT(spec, d2);

		try (
				QEPDataset dataset1 = new QEPDataset(null, "d1", d1, HDTManager.mapHDT(d1), EmptyBitmap.of(0), EMPTY_DELTA);
				QEPDataset dataset2 = new QEPDataset(null, "d2", d2, HDTManager.mapHDT(d2), EmptyBitmap.of(0), EMPTY_DELTA);
				QEPMap map = new QEPMap(root.resolve("maps"), dataset1, dataset2)
		) {
			map.sync();
		}

		try (
				QEPDataset dataset1 = new QEPDataset(null, "d1", d1, HDTManager.mapHDT(d1), EmptyBitmap.of(0), EMPTY_DELTA);
				QEPDataset dataset2 = new QEPDataset(null, "d2", d2, HDTManager.mapHDT(d2), EmptyBitmap.of(0), EMPTY_DELTA);
				QEPMap map = new QEPMap(root.resolve("maps"), dataset1, dataset2)
		) {
			map.sync();
		}
	}

	@Test(expected = IOException.class)
	public void linkReloadErrTest() throws IOException, ParserException {
		LargeFakeDataSetStreamSupplier supplier
				= LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(10_000, 34)
				.withMaxElementSplit(50)
				.withMaxLiteralSize(100)
				.withUnicode(true);


		HDTOptions spec = HDTOptions.of(
				HDTOptionsKeys.DICTIONARY_TYPE_KEY, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS,
				HDTOptionsKeys.LOADER_TYPE_KEY, HDTOptionsKeys.LOADER_TYPE_VALUE_DISK,
				HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, root.resolve("gen")
		);
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

		try (
				QEPDataset dataset1 = new QEPDataset(null, "d1", d1, HDTManager.mapHDT(d1), EmptyBitmap.of(0), EMPTY_DELTA);
				QEPDataset dataset2 = new QEPDataset(null, "d2", d2, HDTManager.mapHDT(d2), EmptyBitmap.of(0), EMPTY_DELTA);
				QEPMap map = new QEPMap(root.resolve("maps"), dataset1, dataset2)
		) {
			map.sync();
		}

		try (
				// use same id with different HDT to create an error
				QEPDataset dataset1 = new QEPDataset(null, "d1", d1, HDTManager.mapHDT(d3), EmptyBitmap.of(0), EMPTY_DELTA);
				QEPDataset dataset2 = new QEPDataset(null, "d2", d2, HDTManager.mapHDT(d4), EmptyBitmap.of(0), EMPTY_DELTA);
				QEPMap map = new QEPMap(root.resolve("maps"), dataset1, dataset2)
		) {
			map.sync();
		}
	}
}