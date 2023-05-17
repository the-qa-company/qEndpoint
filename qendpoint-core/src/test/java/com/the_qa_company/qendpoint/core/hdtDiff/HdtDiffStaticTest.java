package com.the_qa_company.qendpoint.core.hdtDiff;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.options.HDTSpecification;
import com.the_qa_company.qendpoint.core.triples.impl.utils.HDTTestUtils;
import com.the_qa_company.qendpoint.core.util.io.AbstractMapMemoryTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

@RunWith(Parameterized.class)
public class HdtDiffStaticTest extends AbstractMapMemoryTest {
	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> genParam() {
		List<Object[]> list = new ArrayList<>();
		for (HdtDiffTest.DictionaryTestData data : HdtDiffTest.DICTIONARY_TEST_DATA) {
			list.add(new Object[] { data.dictionaryType, data.dictionaryTempType });
		}
		return list;
	}

	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();
	final HDTSpecification spec;

	public HdtDiffStaticTest(String dictionaryType, String tempDictionaryImpl) {
		spec = new HDTSpecification();
		spec.set(HDTOptionsKeys.DICTIONARY_TYPE_KEY, dictionaryType);
		spec.set(HDTOptionsKeys.TEMP_DICTIONARY_IMPL_KEY, tempDictionaryImpl);
	}

	private void ntFilesDiffTest(String a, String b, String amb) throws IOException, ParserException {
		ClassLoader classLoader = getClass().getClassLoader();
		String file1 = Objects.requireNonNull(classLoader.getResource(a), "Can't find " + a).getFile();
		String file2 = Objects.requireNonNull(classLoader.getResource(b), "Can't find " + b).getFile();
		String diff = Objects.requireNonNull(classLoader.getResource(amb), "Can't find " + amb).getFile();

		String hdtFile1 = tempDir.newFile().getAbsolutePath();
		String hdtFile2 = tempDir.newFile().getAbsolutePath();

		try (HDT hdt = HDTManager.generateHDT(file1, HDTTestUtils.BASE_URI, RDFNotation.NTRIPLES, spec, null)) {
			hdt.saveToHDT(hdtFile1, null);
		}

		try (HDT hdt = HDTManager.generateHDT(file2, HDTTestUtils.BASE_URI, RDFNotation.NTRIPLES, spec, null)) {
			hdt.saveToHDT(hdtFile2, null);
		}

		try (HDT hdtDiffExcepted = HDTManager.generateHDT(diff, HDTTestUtils.BASE_URI, RDFNotation.NTRIPLES, spec,
				null); HDT hdtDiffActual = HDTManager.diffHDT(hdtFile1, hdtFile2, spec, null)) {

			Assert.assertEquals("Dictionaries aren't the same", hdtDiffExcepted.getDictionary().getType(),
					hdtDiffActual.getDictionary().getType());

			HdtDiffTest.assertHdtEquals(hdtDiffExcepted, hdtDiffActual);
		}
	}

	@Test
	public void ntFilesDiffTest1M2() throws IOException, ParserException {
		ntFilesDiffTest("hdtDiff/example1.nt", "hdtDiff/example2.nt", "hdtDiff/example1-2.nt");
	}

	@Test
	public void ntFilesDiffTest3M4() throws IOException, ParserException {
		ntFilesDiffTest("hdtDiff/example3.nt", "hdtDiff/example4.nt", "hdtDiff/example3-4.nt");
	}

	@Test
	public void ntFilesDiffTest5M6() throws IOException, ParserException {
		ntFilesDiffTest("hdtDiff/example5.nt", "hdtDiff/example6.nt", "hdtDiff/example5-6.nt");
	}

	@Test
	public void ntFilesDiffTest7M8() throws IOException, ParserException {
		ntFilesDiffTest("hdtDiff/example7.nt", "hdtDiff/example8.nt", "hdtDiff/example7-8.nt");
	}

	@Test
	public void ntFilesDiffTest9M10() throws IOException, ParserException {
		ntFilesDiffTest("hdtDiff/example9.nt", "hdtDiff/example10.nt", "hdtDiff/example9-10.nt");
	}

	@Test
	public void ntFilesDiffTest11M12() throws IOException, ParserException {
		ntFilesDiffTest("hdtDiff/example11.nt", "hdtDiff/example12.nt", "hdtDiff/example11-12.nt");
	}

	@Test
	public void ntFilesDiffTest13M14() throws IOException, ParserException {
		ntFilesDiffTest("hdtDiff/example13.nt", "hdtDiff/example14.nt", "hdtDiff/example13-14.nt");
	}

	@Test
	public void ntFilesDiffTest15M16() throws IOException, ParserException {
		ntFilesDiffTest("hdtDiff/example15.nt", "hdtDiff/example16.nt", "hdtDiff/example15-16.nt");
	}

	@Test
	public void ntFilesDiffTest17M18() throws IOException, ParserException {
		ntFilesDiffTest("hdtDiff/example17.nt", "hdtDiff/example18.nt", "hdtDiff/example17-18.nt");
	}

	@Test
	public void ntFilesDiffTest19M20() throws IOException, ParserException {
		ntFilesDiffTest("hdtDiff/example19.nt", "hdtDiff/example20.nt", "hdtDiff/example19-20.nt");
	}

	@Test
	public void ntFilesDiffTest21M22() throws IOException, ParserException {
		ntFilesDiffTest("hdtDiff/example21.nt", "hdtDiff/example22.nt", "hdtDiff/example21-22.nt");
	}

	@Test
	public void ntFilesDiffTest23M24() throws IOException, ParserException {
		ntFilesDiffTest("hdtDiff/example23.nt", "hdtDiff/example24.nt", "hdtDiff/example23-24.nt");
	}

	@Test
	public void ntFilesDiffTest25M26() throws IOException, ParserException {
		ntFilesDiffTest("hdtDiff/example25.nt", "hdtDiff/example26.nt", "hdtDiff/example25-26.nt");
	}

	@Test
	public void ntFilesDiffTest27M28() throws IOException, ParserException {
		ntFilesDiffTest("hdtDiff/example27.nt", "hdtDiff/example28.nt", "hdtDiff/example27-28.nt");
	}
}
