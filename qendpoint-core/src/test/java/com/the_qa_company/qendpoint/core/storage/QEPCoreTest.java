package com.the_qa_company.qendpoint.core.storage;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap64Big;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;
import com.the_qa_company.qendpoint.core.enums.DictionarySectionRole;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.storage.search.QEPComponentTriple;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import org.apache.commons.io.file.PathUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class QEPCoreTest {
	@Parameterized.Parameters(name = "test")
	public static Collection<Object[]> params() throws IOException, ParserException {
		Path root = Files.createTempDirectory("qepCoreTest");
		Path rootHDT;
		Path[] splitHDT;
		try {

			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxTriples(10_000, 34).withMaxElementSplit(20).withMaxLiteralSize(100);

			HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
					HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS, HDTOptionsKeys.LOADER_TYPE_KEY,
					HDTOptionsKeys.LOADER_TYPE_VALUE_DISK, HDTOptionsKeys.LOADER_DISK_LOCATION_KEY,
					root.resolve("gen"));

			rootHDT = root.resolve("root.hdt");
			try (HDT fakeHDT = supplier.createFakeHDT(spec)) {
				fakeHDT.saveToHDT(rootHDT, ProgressListener.ignore());
			}

			splitHDT = IOUtil.splitHDT(rootHDT, root.resolve("split"), 10);

		} catch (Throwable t) {
			PathUtils.deleteDirectory(root);
			throw t;
		}
		return List.of(new Object[][]{{root, rootHDT, splitHDT}});
	}

	@Parameterized.AfterParam
	public static void after(Path root, Path rootHDT, Path[] splitHDT) throws IOException {
		PathUtils.deleteDirectory(root);
	}

	@Parameterized.Parameter
	public Path root;
	@Parameterized.Parameter(1)
	public Path rootHDT;
	@Parameterized.Parameter(2)
	public Path[] splitHDT;
	Path[] movedFile;

	Path coreRoot;

	@Before
	public void fillStore() throws IOException {
		coreRoot = root.resolve("core");
		if (Files.exists(coreRoot)) {
			PathUtils.deleteDirectory(coreRoot);
		}
		Path store = coreRoot.resolve(QEPCore.FILE_DATASET_STORE);
		Files.createDirectories(coreRoot.resolve(store));
		movedFile = new Path[splitHDT.length];
		for (int i = 0; i < splitHDT.length; i++) {
			movedFile[i] = store.resolve(QEPCore.FILE_DATASET_PREFIX + i + QEPCore.FILE_DATASET_SUFFIX);
		}

		for (int i = 0; i < movedFile.length; i++) {
			Files.move(splitHDT[i], movedFile[i]);
		}
	}

	@After
	public void clearStore() throws IOException {
		for (int i = 0; i < movedFile.length; i++) {
			Files.move(movedFile[i], splitHDT[i]);
		}
		PathUtils.deleteDirectory(coreRoot);
	}

	@Test
	public void coreInitTest() throws QEPCoreException, IOException {
		try (HDT hdt = HDTManager.mapHDT(rootHDT); QEPCore core = new QEPCore(coreRoot, HDTOptions.of())) {
			assertEquals(hdt.getTriples().getNumberOfElements(), core.triplesCount());
		}
	}

	@Test
	public void coreSearchTest() throws QEPCoreException, IOException, NotFoundException {
		try (HDT hdt = HDTManager.mapHDT(rootHDT);
		     QEPCore core = new QEPCore(coreRoot, HDTOptions.of());
		     Bitmap64Big findBM = Bitmap64Big.memory(hdt.getTriples().getNumberOfElements())) {
			assertEquals(hdt.getTriples().getNumberOfElements(), core.triplesCount());
			Iterator<? extends QEPComponentTriple> search = core.search("", "", "");

			long count = 0;
			while (search.hasNext()) {
				QEPComponentTriple next = search.next();
				// convert to a triple string to search over the main HDT
				TripleString ts = next.tripleString();
				count++;
				// search the ts
				IteratorTripleString searchIt = hdt.search(ts.getSubject(), ts.getPredicate(), ts.getObject());
				assertTrue("missing triple for " + ts + " in main HDT", searchIt.hasNext());
				searchIt.next();

				long position = searchIt.getLastTriplePosition();
				assertFalse("position " + position + " was already checked", findBM.access(position));

				findBM.set(position, true);

				assertFalse("multiple find, wtf?", searchIt.hasNext());
			}

			assertEquals("the searched number of values isn't the same as the number of elements in the HDT",
					hdt.getTriples().getNumberOfElements(), count);
		}
	}

	@Test
	public void coreDatasetSearchTest() throws QEPCoreException, IOException, NotFoundException {
		try (HDT hdt = HDTManager.mapHDT(rootHDT); QEPCore core = new QEPCore(coreRoot, HDTOptions.of())) {
			assertEquals(hdt.getTriples().getNumberOfElements(), core.triplesCount());

			for (QEPDataset dataset : core.getDatasets()) {
				long countExcepted = dataset.dataset().getTriples().getNumberOfElements();

				Iterator<QEPComponentTriple> search = dataset.search("", "", "");

				long count = 0;
				while (search.hasNext()) {
					search.next();
					count++;
				}
				assertEquals("unexpected count for dataset " + dataset, countExcepted, count);
			}
		}
	}

	@Test
	public void coreSearchInvTest() throws QEPCoreException, IOException, NotFoundException {
		try (HDT hdt = HDTManager.mapHDT(rootHDT); QEPCore core = new QEPCore(coreRoot, HDTOptions.of())) {
			Map<Integer, Bitmap64Big> bitmaps = new HashMap<>();

			core.getDatasets().forEach(qds -> {
				bitmaps.put(qds.uid(), Bitmap64Big.memory(qds.dataset().getTriples().getNumberOfElements()));
			});

			// Bitmap64Big.memory(hdt.getTriples().getNumberOfElements())
			assertEquals(hdt.getTriples().getNumberOfElements(), core.triplesCount());
			Iterator<? extends TripleString> search = hdt.search("", "", "");

			long count = 0;
			while (search.hasNext()) {
				TripleString ts = search.next();
				// convert to a triple string to search over the main HDT
				count++;
				// search the ts
				Iterator<? extends QEPComponentTriple> searchIt = core.search(ts);
				assertTrue("missing triple for " + ts + " in core", searchIt.hasNext());

				QEPComponentTriple qts = searchIt.next();
				long position = qts.getId();
				int datasetId = qts.getDatasetId();
				Bitmap64Big bitmap = bitmaps.get(datasetId);

				assertNotNull("empty bitmap for dataset: " + datasetId, bitmap);
				assertFalse("position " + position + " was already checked for dataset " + datasetId,
						bitmap.access(position));

				bitmap.set(position, true);

				assertFalse("multiple find, wtf?", searchIt.hasNext());
			}

			assertEquals("the searched number of values isn't the same as the number of elements in the HDT",
					hdt.getTriples().getNumberOfElements(), count);
		}
	}

	private void checkSection(DictionarySectionRole drole, QEPCore core, DictionarySection section)
			throws QEPCoreException {
		TripleComponentRole role = drole.asTripleComponentRole();
		List<QEPDataset> datasets = core.getDatasets().stream().toList();

		Iterator<? extends CharSequence> it = section.getSortedEntries();

		long n = 0;
		while (it.hasNext()) {
			CharSequence component = it.next();
			QEPComponent qepComponent = core.createComponentByString(component);

			for (QEPDataset dataset : datasets) {
				n++;
				long guessedId = dataset.dataset().getDictionary().stringToId(component, role);

				if (guessedId < 0) {
					guessedId = 0;
				}

				QEPComponent mappedComponent = core.createComponentByString(component);
				long mappedId = mappedComponent.getId(dataset.uid(), role);
				if (guessedId != mappedId) {
					throw new AssertionError("""
							%d != %d for role %s
							%s
							""".formatted(guessedId, mappedId, role, mappedComponent.dumpBinding())
					);
				}

				long actualId = qepComponent.getId(dataset.uid(), role);
				if (guessedId != actualId) {
					throw new AssertionError("""
							ids aren't the same for component %s
							(%d / %s / %s)
							""".formatted(qepComponent.dumpBinding(), n, drole, dataset)
					);
				}
			}
			// reverse the search order
			qepComponent = core.createComponentByString(component);
			for (int i = datasets.size() - 1; i >= 0; i--) {
				QEPDataset dataset = datasets.get(i);
				n++;
				long guessedId = dataset.dataset().getDictionary().stringToId(component, role);

				if (guessedId < 0) {
					guessedId = 0;
				}

				QEPComponent mappedComponent = core.createComponentByString(component);
				long mappedId = mappedComponent.getId(dataset.uid(), role);
				if (guessedId != mappedId) {
					throw new AssertionError("""
							%d != %d for role %s
							%s
							""".formatted(guessedId, mappedId, role, mappedComponent.dumpBinding())
					);
				}

				long actualId = qepComponent.getId(dataset.uid(), role);
				if (guessedId != actualId) {
					throw new AssertionError("""
							%d != %d
							(%d / %s / %s)
							find: %s
							component: %s
							ids aren't the same for component
							%s
							""".formatted(guessedId, actualId, n, drole, dataset,
							dataset.find(component), component,
							qepComponent.dumpBinding())
					);
				}
			}
		}
	}

	@Test
	public void coreDictionaryTest() throws QEPCoreException, IOException {
		try (HDT hdt = HDTManager.mapHDT(rootHDT); QEPCore core = new QEPCore(coreRoot, HDTOptions.of())) {
			checkSection(DictionarySectionRole.PREDICATE, core, hdt.getDictionary().getPredicates());
			checkSection(DictionarySectionRole.SUBJECT, core, hdt.getDictionary().getSubjects());
			checkSection(DictionarySectionRole.SHARED, core, hdt.getDictionary().getShared());
			checkSection(DictionarySectionRole.OBJECT, core, hdt.getDictionary().getShared());
			for (DictionarySection section : hdt.getDictionary().getAllObjects().values()) {
				checkSection(DictionarySectionRole.OBJECT, core, section);
			}
		}
	}

	@Test
	public void coreDictionaryObjectTest() throws QEPCoreException, IOException {
		try (HDT hdt = HDTManager.mapHDT(rootHDT); QEPCore core = new QEPCore(coreRoot, HDTOptions.of())) {
			for (DictionarySection section : hdt.getDictionary().getAllObjects().values()) {
				checkSection(DictionarySectionRole.OBJECT, core, section);
			}
			checkSection(DictionarySectionRole.OBJECT, core, hdt.getDictionary().getShared());
		}
	}

	@Test
	public void coreDictionaryPredicateTest() throws QEPCoreException, IOException {
		try (HDT hdt = HDTManager.mapHDT(rootHDT); QEPCore core = new QEPCore(coreRoot, HDTOptions.of())) {
			checkSection(DictionarySectionRole.PREDICATE, core, hdt.getDictionary().getPredicates());
		}
	}

	@Test
	public void coreDictionarySubjectTest() throws QEPCoreException, IOException {
		try (HDT hdt = HDTManager.mapHDT(rootHDT); QEPCore core = new QEPCore(coreRoot, HDTOptions.of())) {
			checkSection(DictionarySectionRole.SUBJECT, core, hdt.getDictionary().getSubjects());
			checkSection(DictionarySectionRole.SHARED, core, hdt.getDictionary().getShared());
		}
	}

}
