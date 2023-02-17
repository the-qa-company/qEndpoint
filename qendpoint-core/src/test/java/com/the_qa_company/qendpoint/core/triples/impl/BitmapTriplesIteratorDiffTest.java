package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.compact.bitmap.BitmapFactory;
import com.the_qa_company.qendpoint.core.compact.bitmap.ModifiableBitmap;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.hdt.writer.TripleWriterHDT;
import com.the_qa_company.qendpoint.core.hdtDiff.utils.TripleStringUtility;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.options.HDTSpecification;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class BitmapTriplesIteratorDiffTest {
	/**
	 * Return type for
	 * {@link #createTestHDT(File, File, int, int, int, int, int)}
	 */
	public static class HDTDiffData {
		public int tupleCountA;
		public int tupleCountB;
		public int shared;
	}

	private static final Logger logger = LoggerFactory.getLogger(BitmapTriplesIteratorDiffTest.class);

	/**
	 * create a 2 test hdts and return the tuple count
	 *
	 * @param fileName   hdt file name
	 * @param fileName2  hdt 2 file name
	 * @param subjects   number of subjects
	 * @param predicates number of predicates
	 * @param objects    number of objects
	 * @param shared     number of shared subjects/objects
	 * @return tuple count
	 */
	public static HDTDiffData createTestHDT(File fileName, File fileName2, int subjects, int predicates, int objects,
			int shared, int shift) throws IOException {
		String baseURI = "http://ex.org/";
		HDTDiffData count = new HDTDiffData();
		int shiftCount = 0;
		try (final TripleWriterHDT wr = new TripleWriterHDT(baseURI, new HDTSpecification(), fileName.getAbsolutePath(),
				false);
				final TripleWriterHDT wr2 = new TripleWriterHDT(baseURI, new HDTSpecification(),
						fileName2.getAbsolutePath(), false)) {
			for (int i = subjects; i > 0; i--) {
				String subject;
				if (i <= shared) {
					subject = baseURI + "Shared" + i;
				} else {
					subject = baseURI + "Subject" + (i - shared);
				}

				for (int j = predicates; j > 0; j--) {
					String predicate = baseURI + "Predicate" + j;
					for (int k = objects; k > 0; k--) {
						String object;
						if (k <= shared) {
							object = baseURI + "Shared" + k;
						} else {
							object = baseURI + "Object" + (k - shared);
						}
						shiftCount = (shiftCount + 1) % shift;
						int newtriple = 0;

						if (shiftCount != 0) {
							wr.addTriple(new TripleString(subject, predicate, object));
							count.tupleCountA++;
							newtriple++;
						}
						if (shiftCount == 0 || shiftCount == 1) {
							wr2.addTriple(new TripleString(subject, predicate, object));
							count.tupleCountB++;
							newtriple++;
						}

						if (newtriple == 2) {
							count.shared++;
						}
					}
				}
			}
		}

		return count;
	}

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object> genParam() {
		return Arrays.asList(HDTVocabulary.DICTIONARY_TYPE_FOUR_SECTION,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_SECTION_BIG,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS, HDTVocabulary.DICTIONARY_TYPE_FOUR_PSFC_SECTION);
	}

	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();
	final HDTSpecification spec;

	public BitmapTriplesIteratorDiffTest(String dictionaryType) {
		spec = new HDTSpecification();
		spec.set(HDTOptionsKeys.DICTIONARY_TYPE_KEY, dictionaryType);
	}

	@Test
	public void createTestHDTTest() throws IOException {
		File hdt1 = tempDir.newFile();
		File hdt2 = tempDir.newFile();

		HDTDiffData data = createTestHDT(hdt1, hdt2, 40, 50, 60, 5, 4);

		HDT origin = HDTManager.mapHDT(hdt2.getAbsolutePath());
		HDT diff = HDTManager.mapHDT(hdt1.getAbsolutePath());

		Assert.assertEquals("Count in orig hdt", data.tupleCountB, origin.getTriples().getNumberOfElements());
		Assert.assertEquals("Count in diff hdt", data.tupleCountA, diff.getTriples().getNumberOfElements());

		IteratorTripleID it1 = origin.getTriples().searchAll();

		TripleStringUtility triple = new TripleStringUtility(origin);
		long shared = 0;
		while (it1.hasNext()) {
			triple.loadTriple(it1.next());
			TripleID other = triple.searchInto(diff);
			if (other != null) {
				shared++;
			}
		}
		Assert.assertEquals("Shared triples", data.shared, shared);
	}

	@Test
	public void diffTest() throws IOException {
		File hdt1 = tempDir.newFile();
		File hdt2 = tempDir.newFile();

		HDTDiffData data = createTestHDT(hdt1, hdt2, 100, 50, 60, 5, 4);

		HDT origin = HDTManager.mapHDT(hdt1.getAbsolutePath(), null, spec);
		HDT diff = HDTManager.mapHDT(hdt2.getAbsolutePath(), null, spec);

		ModifiableBitmap bitmap = BitmapFactory.createRWBitmap(data.tupleCountA);

		BitmapTriplesIteratorDiff d = new BitmapTriplesIteratorDiff(origin, diff, bitmap);

		d.fillBitmap();

		System.out.println("Bitmap bits: " + bitmap.countOnes());
		System.out.println("tupleCountA: " + data.tupleCountA);
		System.out.println("tupleCountB: " + data.tupleCountB);
		System.out.println("shared:      " + data.shared);
		Assert.assertEquals("Shared element", data.shared, bitmap.countOnes());

		IteratorTripleID it = origin.getTriples().searchAll();

		TripleStringUtility triple = new TripleStringUtility(origin);
		while (it.hasNext()) {
			triple.loadTriple(it.next());
			long index = it.getLastTriplePosition();
			if (triple.searchInto(diff) == null) {
				Assert.assertFalse("triple bitmap", bitmap.access(index));
			} else {
				Assert.assertTrue("triple bitmap", bitmap.access(index));
			}
		}
	}
}
