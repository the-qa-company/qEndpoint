package com.the_qa_company.qendpoint.core.dictionary.impl;

import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.hdt.HDTManagerTest;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import com.the_qa_company.qendpoint.core.util.string.PrefixesStorage;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

import static com.the_qa_company.qendpoint.core.hdt.HDTManagerTest.HDTManagerTestBase.assertEqualsHDT;
import static org.junit.Assert.*;

public class MultipleSectionDictionaryLangPrefixesTest {
	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void initTest() throws IOException, ParserException, NotFoundException {
		Path root = tempDir.newFolder().toPath();
		final int count = 10000;
		LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(count, 42)
				.withMaxLiteralSize(20).withMaxElementSplit(50);
		Path hdtPath = root.resolve("test.hdt");
		Path hdtPath2 = root.resolve("test2.hdt");
		Path hdtPath12 = root.resolve("test12.hdt");

		PrefixesStorage prefixes = supplier.createPrefixStorage();
		// prefixes.dump();
		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG_PREFIXES, HDTOptionsKeys.LOADER_PREFIXES,
				prefixes.saveConfig(), HDTOptionsKeys.LOADER_TYPE_KEY, HDTOptionsKeys.LOADER_TYPE_VALUE_DISK,
				HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, root.resolve("work"),
				HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY, root.resolve("gd.hdt"),
				"debug.kcatimpl.checkFastCatPref", true

		);
		try (HDT hdt = HDTManager.generateHDT(supplier.createTripleStringStream(),
				LargeFakeDataSetStreamSupplier.BASE_URI, spec, ProgressListener.ignore())) {
			hdt.saveToHDT(hdtPath);
		}
		try (HDT hdt = HDTManager.generateHDT(supplier.createTripleStringStream(),
				LargeFakeDataSetStreamSupplier.BASE_URI, spec, ProgressListener.ignore())) {
			hdt.saveToHDT(hdtPath2);
		}

		supplier.withMaxTriples(count * 2);
		supplier.reset();
		try (HDT hdt = HDTManager.generateHDT(supplier.createTripleStringStream(),
				LargeFakeDataSetStreamSupplier.BASE_URI, spec, ProgressListener.ignore())) {
			hdt.saveToHDT(hdtPath12);
		}
		supplier.withMaxTriples(count);

		try (HDT hdt = HDTManager.mapHDT(hdtPath);
				HDT hdt2 = HDTManager.mapHDT(hdtPath2);
				HDT hdt12 = HDTManager.mapHDT(hdtPath12)) {
			Dictionary dict = hdt.getDictionary();
			assertTrue("bad type: " + dict.getClass().getCanonicalName(),
					dict instanceof MultipleSectionDictionaryLangPrefixes);
			HDTManagerTest.HDTManagerTestBase.checkHDTConsistency(hdt);
			Dictionary dict2 = hdt2.getDictionary();
			assertTrue("bad type: " + dict2.getClass().getCanonicalName(),
					dict2 instanceof MultipleSectionDictionaryLangPrefixes);
			HDTManagerTest.HDTManagerTestBase.checkHDTConsistency(hdt2);
			Dictionary dict12 = hdt12.getDictionary();
			assertTrue("bad type: " + dict12.getClass().getCanonicalName(),
					dict12 instanceof MultipleSectionDictionaryLangPrefixes);
			HDTManagerTest.HDTManagerTestBase.checkHDTConsistency(hdt12);
			supplier.reset();
			{

				Iterator<TripleString> it = supplier.createTripleStringStream();

				long id = 0;
				while (it.hasNext()) {
					id++;
					TripleString ts = it.next();
					ts.setAll(LiteralsUtils.resToPrefLangCut(ts.getSubject(), prefixes),
							LiteralsUtils.resToPrefLangCut(ts.getPredicate(), prefixes),
							LiteralsUtils.resToPrefLangCut(ts.getObject(), prefixes) // we
																						// don't
																						// map
																						// the
																						// literals
					);

					TripleID tid = dict.toTripleId(ts);
					assertTrue("#" + id + " - invalid tid for " + ts + " / " + tid, tid.isValid());
					assertTrue(hdt.getTriples().search(tid).hasNext());
				}
			}
			{
				Iterator<TripleString> it = supplier.createTripleStringStream();

				long id = 0;
				while (it.hasNext()) {
					id++;
					TripleString ts = it.next();
					ts.setAll(LiteralsUtils.resToPrefLangCut(ts.getSubject(), prefixes),
							LiteralsUtils.resToPrefLangCut(ts.getPredicate(), prefixes),
							LiteralsUtils.resToPrefLangCut(ts.getObject(), prefixes) // we
																						// don't
																						// map
																						// the
																						// literals
					);

					TripleID tid = dict2.toTripleId(ts);
					assertTrue("#" + id + " - invalid tid for " + ts + " / " + tid, tid.isValid());
					assertTrue(hdt2.getTriples().search(tid).hasNext());
				}
			}
			supplier.withMaxTriples(count * 2);
			supplier.reset();
			{
				Iterator<TripleString> it = supplier.createTripleStringStream();

				long id = 0;
				while (it.hasNext()) {
					id++;
					TripleString ts = it.next();
					ts.setAll(LiteralsUtils.resToPrefLangCut(ts.getSubject(), prefixes),
							LiteralsUtils.resToPrefLangCut(ts.getPredicate(), prefixes),
							LiteralsUtils.resToPrefLangCut(ts.getObject(), prefixes) // we
																						// don't
																						// map
																						// the
																						// literals
					);

					TripleID tid = dict12.toTripleId(ts);
					assertTrue("#" + id + " - invalid tid for " + ts + " / " + tid, tid.isValid());
					assertTrue(hdt12.getTriples().search(tid).hasNext());
				}
			}
		}

		Path hdtPathCat = root.resolve("cat.hdt");
		try (HDT hdt = HDTManager.catHDTPath(List.of(hdtPath, hdtPath2), spec, ProgressListener.ignore())) {
			hdt.saveToHDT(hdtPathCat);
		}
		try (HDT cat = HDTManager.mapHDT(hdtPathCat); HDT exc = HDTManager.mapHDT(hdtPath12)) {

			assertEqualsHDT(exc, cat);
		}

	}

	@Test
	public void genTests() throws IOException, ParserException {
		Path root = tempDir.newFolder().toPath();
		final int count = 10000;
		LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(count, 42)
				.withMaxLiteralSize(20).withMaxElementSplit(50);
		Path hdtPath = root.resolve("test.hdt");
		Path hdtPath2 = root.resolve("test2.hdt");

		PrefixesStorage prefixes = supplier.createPrefixStorage();
		// prefixes.dump();
		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG_PREFIXES, HDTOptionsKeys.LOADER_PREFIXES,
				prefixes.saveConfig(), HDTOptionsKeys.LOADER_TYPE_KEY, HDTOptionsKeys.LOADER_TYPE_VALUE_DISK,
				HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, root.resolve("work"),
				HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY, root.resolve("gd.hdt"),
				"debug.kcatimpl.checkFastCatPref", true);
		try (HDT hdt = HDTManager.generateHDT(supplier.createTripleStringStream(),
				LargeFakeDataSetStreamSupplier.BASE_URI, spec, ProgressListener.ignore())) {
			hdt.saveToHDT(hdtPath);
		}
		supplier.reset();
		HDTOptions spec2 = spec.pushTop();
		spec2.set(HDTOptionsKeys.DICTIONARY_TYPE_KEY, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG);
		try (HDT hdt = HDTManager.generateHDT(supplier.createTripleStringStream(),
				LargeFakeDataSetStreamSupplier.BASE_URI, spec2, ProgressListener.ignore())) {
			hdt.saveToHDT(hdtPath2);
		}

		try (HDT hdt1 = HDTManager.mapHDT(hdtPath); HDT hdt2 = HDTManager.mapHDT(hdtPath2)) {
			assertEquals(hdt2.getTriples().getNumberOfElements(), hdt1.getTriples().getNumberOfElements());

			Dictionary dict1 = hdt1.getDictionary();
			Dictionary dict2 = hdt2.getDictionary();
			assertTrue("bad type: " + dict1.getClass().getCanonicalName(),
					dict1 instanceof MultipleSectionDictionaryLangPrefixes);
			assertTrue("bad type: " + dict2.getClass().getCanonicalName(),
					dict2 instanceof MultipleSectionDictionaryLang);

			IteratorTripleID it1 = hdt1.getTriples().searchAll();
			IteratorTripleID it2 = hdt2.getTriples().searchAll();

			// the prefixes should change the order
			while (it1.hasNext()) {
				assertTrue(it2.hasNext());
				TripleID tid1 = it1.next();
				TripleID tid2 = it2.next();
				assertEquals(tid1, tid2);

				TripleString ts1 = dict1.toTripleString(tid1);
				TripleString ts2 = dict2.toTripleString(tid2);

				assertEquals(ts1.toString(), ts2.toString());
			}
			assertFalse(it2.hasNext());
		}
	}

	@Test
	public void genUncutTests() throws IOException, ParserException {
		Path root = tempDir.newFolder().toPath();
		final int count = 10000;
		LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(count, 42)
				.withMaxLiteralSize(20).withMaxElementSplit(50);
		Path hdtPath = root.resolve("test.hdt");
		Path hdtPath2 = root.resolve("test2.hdt");

		PrefixesStorage prefixes = supplier.createPrefixStorage();
		// prefixes.dump();
		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG_PREFIXES, HDTOptionsKeys.LOADER_PREFIXES,
				prefixes.saveConfig(), HDTOptionsKeys.LOADER_TYPE_KEY, HDTOptionsKeys.LOADER_TYPE_VALUE_DISK,
				HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, root.resolve("work"),
				HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY, root.resolve("gd.hdt"));
		try (HDT hdt = HDTManager.generateHDT(supplier.createTripleStringStream(),
				LargeFakeDataSetStreamSupplier.BASE_URI, spec, ProgressListener.ignore())) {
			hdt.saveToHDT(hdtPath);
		}
		supplier.reset();
		HDTOptions spec2 = spec.pushTop();
		spec2.set(HDTOptionsKeys.DICTIONARY_TYPE_KEY, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG);
		try (HDT hdt = HDTManager.generateHDT(supplier.createTripleStringStream(),
				LargeFakeDataSetStreamSupplier.BASE_URI, spec2, ProgressListener.ignore())) {
			hdt.saveToHDT(hdtPath2);
		}

		try (HDT hdt1 = HDTManager.mapHDT(hdtPath); HDT hdt2 = HDTManager.mapHDT(hdtPath2)) {
			assertEquals(hdt2.getTriples().getNumberOfElements(), hdt1.getTriples().getNumberOfElements());

			Dictionary dict1 = hdt1.getDictionary();
			Dictionary dict2 = hdt2.getDictionary();
			assertTrue("bad type: " + dict1.getClass().getCanonicalName(),
					dict1 instanceof MultipleSectionDictionaryLangPrefixes);
			assertTrue("bad type: " + dict2.getClass().getCanonicalName(),
					dict2 instanceof MultipleSectionDictionaryLang);

			((MultipleSectionDictionaryLangPrefixes) dict1).setMapEnd(false);

			IteratorTripleID it1 = hdt1.getTriples().searchAll();
			IteratorTripleID it2 = hdt2.getTriples().searchAll();

			// the prefixes should change the order
			while (it1.hasNext()) {
				assertTrue(it2.hasNext());
				TripleID tid1 = it1.next();
				TripleID tid2 = it2.next();
				assertEquals(tid1, tid2);

				TripleString ts1 = dict1.toTripleString(tid1);
				TripleString ts2 = dict2.toTripleString(tid2);

				// we map the MSDL triple because the MSDLP is formatted
				ts2.setAll(LiteralsUtils.resToPrefLangCut(ts2.getSubject(), prefixes),
						LiteralsUtils.resToPrefLangCut(ts2.getPredicate(), prefixes),
						LiteralsUtils.resToPrefLangCut(ts2.getObject(), prefixes));

				assertEquals(ts1.toString(), ts2.toString());
			}
			assertFalse(it2.hasNext());
		}
	}

}
