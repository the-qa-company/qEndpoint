package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.iterator.DictionaryTranslateIterator;
import com.the_qa_company.qendpoint.core.iterator.DictionaryTranslateIteratorBuffer;
import com.the_qa_company.qendpoint.core.iterator.SequentialSearchIteratorTripleID;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.options.HDTSpecification;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.triples.impl.utils.HDTTestUtils;
import com.the_qa_company.qendpoint.core.util.io.AbstractMapMemoryTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.*;

@RunWith(Parameterized.class)
public class BitmapTriplesIteratorPositionTest extends AbstractMapMemoryTest {

	public static final List<String> DICTIONARIES = List.of(HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_SECTION,
			HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_SECTION_BIG, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS,
			HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_PSFC_SECTION);

	private static final Field ITERATOR_SUB;
	private static final Field ITERATOR_SUB_BUFFER;
	private static final Field ITERATOR_SUB_SEQ;

	static {
		try {
			ITERATOR_SUB = DictionaryTranslateIterator.class.getDeclaredField("iterator");
			ITERATOR_SUB_BUFFER = DictionaryTranslateIteratorBuffer.class.getDeclaredField("iterator");
			ITERATOR_SUB_SEQ = SequentialSearchIteratorTripleID.class.getDeclaredField("iterator");

			ITERATOR_SUB.setAccessible(true);
			ITERATOR_SUB_BUFFER.setAccessible(true);
			ITERATOR_SUB_SEQ.setAccessible(true);
		} catch (NoSuchFieldException e) {
			throw new Error(e);
		}
	}

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> genParam() {
		List<Object[]> lst = new ArrayList<>();
		for (String dict : DICTIONARIES) {
			lst.add(new Object[] { dict, 25, 50, 37, 12 });
			lst.add(new Object[] { dict, 25, 50, 37, 0 });
			lst.add(new Object[] { dict, 25, 50, 0, 12 });
			lst.add(new Object[] { dict, 25, 50, 0, 0 });
			lst.add(new Object[] { dict, 25, 0, 37, 12 });
			lst.add(new Object[] { dict, 25, 0, 37, 0 });
			lst.add(new Object[] { dict, 25, 0, 0, 12 });
			lst.add(new Object[] { dict, 25, 0, 0, 0 });
			lst.add(new Object[] { dict, 0, 50, 37, 12 });
			lst.add(new Object[] { dict, 0, 50, 37, 0 });
			lst.add(new Object[] { dict, 0, 50, 0, 12 });
			lst.add(new Object[] { dict, 0, 50, 0, 0 });
			lst.add(new Object[] { dict, 0, 0, 37, 12 });
			lst.add(new Object[] { dict, 0, 0, 37, 0 });
			lst.add(new Object[] { dict, 0, 0, 0, 12 });
			lst.add(new Object[] { dict, 0, 0, 0, 0 });
		}
		return lst;
	}

	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();
	final HDTSpecification spec;
	final int shared;
	final int subjects;
	final int predicates;
	final int objects;

	public BitmapTriplesIteratorPositionTest(String dictionaryType, int subjects, int predicates, int objects,
			int shared) {
		spec = new HDTSpecification();
		spec.set(HDTOptionsKeys.DICTIONARY_TYPE_KEY, dictionaryType);
		this.subjects = subjects;
		this.predicates = predicates;
		this.objects = objects;
		this.shared = shared;
	}

	@Test
	public void searchAllTest() throws IOException, NotFoundException {
		try (HDTTestUtils data = new HDTTestUtils(tempDir.newFile(), subjects, predicates, objects, shared, spec,
				false)) {

			IteratorTripleString it = data.searchForSPO(0, 0, 0);

			// printIterator(it);

			long index = 0L;
			while (it.hasNext()) {
				TripleString triple = it.next();
				long tripleIndex = it.getLastTriplePosition();

				// test if the search is returning index from 0, 1, ...count
				Assert.assertEquals("nextTriplePosition order", index++, tripleIndex);

				// test if the triple is at the right index
				HDTTestUtils.SpoId spoId = data.tripleToSpo(triple);
				long testIndex = spoId.getIndex();
				Assert.assertEquals("getIndex hdt value", testIndex, tripleIndex);

				TripleID findTriple = data.hdt.getTriples().findTriple(tripleIndex);
				long testIndex2 = data.tripleToSpo(findTriple).getIndex();
				Assert.assertEquals("getIndex findTriple hdt value", testIndex2, tripleIndex);
			}
		}
	}

	@Test
	public void searchAllTestBuffer() throws IOException, NotFoundException {
		try (HDTTestUtils data = new HDTTestUtils(tempDir.newFile(), subjects, predicates, objects, shared, spec,
				true)) {

			IteratorTripleString it = data.searchForSPO(0, 0, 0);

			// printIterator(it);

			long index = 0L;
			while (it.hasNext()) {
				TripleString triple = it.next();
				long tripleIndex = it.getLastTriplePosition();

				// test if the search is returning index from 0, 1, ...count
				Assert.assertEquals("nextTriplePosition order", index++, tripleIndex);

				// test if the triple is at the right index
				HDTTestUtils.SpoId spoId = data.tripleToSpo(triple);
				long testIndex = spoId.getIndex();
				Assert.assertEquals("getIndex hdt value", testIndex, tripleIndex);

				// test if we can find it back again
				TripleID findTriple = data.hdt.getTriples().findTriple(tripleIndex);
				long testIndex2 = data.tripleToSpo(findTriple).getIndex();
				Assert.assertEquals("getIndex findTriple hdt value", testIndex2, tripleIndex);
			}
		}
	}

	/**
	 * create a test for a particular spo pattern
	 *
	 * @param s subject to search (or 0 for wildcard)
	 * @param p predicate to search (or 0 for wildcard)
	 * @param o object to search (or 0 for wildcard)
	 * @throws IOException       file
	 * @throws NotFoundException search
	 */
	private void searchTest(int s, int p, int o) throws IOException, NotFoundException {
		try (HDTTestUtils data = new HDTTestUtils(tempDir.newFile(), subjects, predicates, objects, shared, spec,
				true)) {

			IteratorTripleString it = data.searchForSPO(s, p, o);

			// printIterator(it);

			while (it.hasNext()) {
				TripleString triple = it.next();
				long tripleIndex = it.getLastTriplePosition();
				// test if the triple is at the right index
				HDTTestUtils.SpoId spoId = data.tripleToSpo(triple);
				long testIndex = spoId.getIndex();
				Assert.assertEquals("getIndex hdt value", testIndex, tripleIndex);
			}
		}
	}

	@Test
	public void ___SearchTest() throws IOException, NotFoundException {
		searchTest(0, 0, 0); // Tested pattern: ???
	}

	@Test
	public void s__SearchTest() throws IOException, NotFoundException {
		searchTest(subjects / 3, 0, 0); // Tested pattern: S??
	}

	@Test
	public void _p_SearchTest() throws IOException, NotFoundException {
		searchTest(0, predicates / 3, 0); // Tested pattern: ?P?
	}

	@Test
	public void sp_SearchTest() throws IOException, NotFoundException {
		searchTest(subjects / 3, predicates / 3, 0); // Tested pattern: SP?
	}

	@Test
	public void __oSearchTest() throws IOException, NotFoundException {
		searchTest(0, 0, objects / 3); // Tested pattern: ??O
	}

	@Test
	public void s_oSearchTest() throws IOException, NotFoundException {
		searchTest(subjects / 3, 0, objects / 3); // Tested pattern: S?O
	}

	@Test
	public void _poSearchTest() throws IOException, NotFoundException {
		searchTest(0, predicates / 3, objects / 3); // Tested pattern: ?PO
	}

	@Test
	public void spoSearchTest() throws IOException, NotFoundException {
		searchTest(subjects / 3, predicates / 3, objects / 3); // Tested
																// pattern: SPO
	}

	private boolean equalsCharSequence(CharSequence cs1, CharSequence cs2) {
		if (cs1.length() != cs2.length())
			return false;

		for (int i = 0; i < cs1.length(); i++)
			if (cs1.charAt(i) != cs2.charAt(i))
				return false;
		return true;
	}

	private boolean equalsTriple(TripleString s1, TripleString s2) {
		// quick fix, might do a pr/issue later to remove it
		// s1.equals(s1) -> false
		return equalsCharSequence(s1.getSubject(), s2.getSubject())
				&& equalsCharSequence(s1.getPredicate(), s2.getPredicate())
				&& equalsCharSequence(s1.getObject(), s2.getObject());

	}

	public long getIndex(List<TripleString> triples, TripleString str) {
		for (int i = 0; i < triples.size(); i++) {
			if (equalsTriple(str, triples.get(i)))
				return i;
		}
		throw new IllegalArgumentException("not a triple or our hdt: " + str);
	}

	private void searchTPSTest(int s, int p, int o) throws NotFoundException, ParserException, IOException {
		ClassLoader classLoader = getClass().getClassLoader();
		File f = new File(tempDir.newFolder(), "test.nt");
		InputStream ntFile = classLoader.getResourceAsStream("example_triplePosition.nt");
		Assert.assertNotNull("ntFile can't be null", ntFile);

		Files.copy(ntFile, f.toPath());
		try (HDT hdt = HDTManager.generateHDT(f.getAbsolutePath(), HDTTestUtils.BASE_URI, RDFNotation.NTRIPLES,
				new HDTSpecification(), null)) {
			List<TripleString> triples = new ArrayList<>();
			hdt.search("", "", "").forEachRemaining(triples::add);
			TripleString ts = triples.get(10);
			CharSequence ss = s == 0 ? "" : ts.getSubject();
			CharSequence sp = p == 0 ? "" : ts.getPredicate();
			CharSequence so = o == 0 ? "" : ts.getObject();

			IteratorTripleString it = hdt.search(ss, sp, so);

			while (it.hasNext()) {
				TripleString tripleString = it.next();
				Assert.assertEquals("Sorted triple index", getIndex(triples, tripleString), it.getLastTriplePosition());
			}
		}
	}

	@Test
	public void ___SearchTPSTest() throws IOException, NotFoundException, ParserException {
		searchTPSTest(0, 0, 0); // Tested pattern: ???
	}

	@Test
	public void s__SearchTPSTest() throws IOException, NotFoundException, ParserException {
		searchTPSTest(1, 0, 0); // Tested pattern: S??
	}

	@Test
	public void _p_SearchTPSTest() throws IOException, NotFoundException, ParserException {
		searchTPSTest(0, 1, 0); // Tested pattern: ?P?
	}

	@Test
	public void sp_SearchTPSTest() throws IOException, NotFoundException, ParserException {
		searchTPSTest(1, 1, 0); // Tested pattern: SP?
	}

	@Test
	public void __oSearchTPSTest() throws IOException, NotFoundException, ParserException {
		searchTPSTest(0, 0, 1); // Tested pattern: ??O
	}

	@Test
	public void s_oSearchTPSTest() throws IOException, NotFoundException, ParserException {
		searchTPSTest(1, 0, 1); // Tested pattern: S?O
	}

	@Test
	public void _poSearchTPSTest() throws IOException, NotFoundException, ParserException {
		searchTPSTest(0, 1, 1); // Tested pattern: ?PO
	}

	@Test
	public void spoSearchTPSTest() throws IOException, NotFoundException, ParserException {
		searchTPSTest(1, 1, 1); // Tested pattern: SPO
	}

}
