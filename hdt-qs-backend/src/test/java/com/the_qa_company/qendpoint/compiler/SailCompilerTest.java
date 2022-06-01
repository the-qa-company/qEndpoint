package com.the_qa_company.qendpoint.compiler;

import com.the_qa_company.qendpoint.utils.sail.FilteringSail;
import com.the_qa_company.qendpoint.utils.sail.MultiTypeFilteringSail;
import com.the_qa_company.qendpoint.utils.sail.SailTest;
import com.the_qa_company.qendpoint.utils.sail.filter.LuceneMatchExprSailFilter;
import com.the_qa_company.qendpoint.utils.sail.filter.OpSailFilter;
import com.the_qa_company.qendpoint.utils.sail.filter.PredicateSailFilter;
import com.the_qa_company.qendpoint.utils.sail.filter.SailFilter;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.NotifyingSail;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.evaluation.TupleFunctionEvaluationMode;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SailCompilerTest {
	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	private LoadData loadFile(String fileName) throws IOException, SailCompiler.SailCompilerException {
		String locationNative = tempDir.newFolder().getAbsolutePath();
		SailCompiler compiler = new SailCompiler();

		try (InputStream is = getClass().getClassLoader().getResourceAsStream(fileName)) {
			compiler.load(is, Rio.getParserFormatForFileName(fileName).orElseThrow());
		}

		compiler.registerDirObject(new ParsedStringObject(locationNative));

		MemoryStore source = new MemoryStore();
		NotifyingSail compiledSail = compiler.compile(source);

		ValueFactory vf = SimpleValueFactory.getInstance();

		SailRepository repo = new SailRepository(compiledSail);
		repo.init();
		try (SailRepositoryConnection connection = repo.getConnection()) {
			connection.add(vf.createStatement(vf.createIRI("http://aaa"), vf.createIRI("http://bbb"),
					vf.createIRI("http://ccc")));
		}
		return new LoadData(compiledSail, compiler, locationNative);
	}

	private void assertLuceneSail(Sail sail, String indexId, String dir, String luceneLang) {
		assertLuceneSail(sail, indexId, dir, luceneLang, new LuceneParam());
	}

	private void assertLuceneSail(Sail sail, String indexId, String dir, String luceneLang, LuceneParam param) {
		Assert.assertTrue(sail instanceof LuceneSail);
		LuceneSail luceneSail = (LuceneSail) sail;
		Assert.assertEquals(indexId, luceneSail.getParameter(LuceneSail.INDEX_ID));
		Assert.assertEquals(dir, luceneSail.getParameter(LuceneSail.LUCENE_DIR_KEY));
		Assert.assertEquals(TupleFunctionEvaluationMode.NATIVE, luceneSail.getEvaluationMode());
		Assert.assertEquals(luceneLang, luceneSail.getParameter(LuceneSail.INDEXEDLANG));
		param.assertParams(luceneSail);
	}

	@Test
	public void loadModel1Test() throws IOException, SailCompiler.SailCompilerException {
		LoadData data = loadFile("model/model_example1.ttl");
		assertLuceneSail(data.sail, SailTest.NAMESPACE + "luceneIndex1",
				data.compiler.parseDir("${locationNative}lucene1"), null);
	}

	@Test
	public void loadModel2Test() throws IOException, SailCompiler.SailCompilerException {
		LoadData data = loadFile("model/model_example2.ttl");
		Assert.assertTrue(data.sail instanceof FilteringSail);
		FilteringSail sail = (FilteringSail) data.sail;
		assertLuceneSail(sail.getOnYesSail(), SailTest.NAMESPACE + "luceneIndex1",
				data.compiler.parseDir("${locationNative}lucene1"), null);
		SailFilter filter = sail.getFilter(null);
		Assert.assertTrue(filter instanceof OpSailFilter);
		OpSailFilter opf = (OpSailFilter) filter;
		Assert.assertSame(opf.getOperation(), OpSailFilter.AND);

		SailFilter filter1 = opf.getFilter1();
		Assert.assertTrue(filter1 instanceof LuceneMatchExprSailFilter);

		SailFilter filter2 = opf.getFilter2();
		Assert.assertTrue(filter2 instanceof OpSailFilter);
		OpSailFilter opf1 = (OpSailFilter) filter2;
		Assert.assertSame(opf1.getOperation(), OpSailFilter.OR);

		SailFilter filter21 = opf1.getFilter1();
		Assert.assertTrue(filter21 instanceof PredicateSailFilter);
		PredicateSailFilter prf1 = (PredicateSailFilter) filter21;
		Assert.assertEquals(prf1.getPredicate(),
				Set.of(data.sail.getValueFactory().createIRI(SailTest.NAMESPACE + "text")));

		SailFilter filter22 = opf1.getFilter2();
		Assert.assertTrue(filter22 instanceof PredicateSailFilter);
		PredicateSailFilter prf2 = (PredicateSailFilter) filter22;
		Assert.assertEquals(prf2.getPredicate(),
				Set.of(data.sail.getValueFactory().createIRI(SailTest.NAMESPACE + "typeof")));
	}

	@Test
	public void loadModel3Test() throws IOException, SailCompiler.SailCompilerException {
		LoadData data = loadFile("model/model_example3.ttl");

		FilteringSail sail = (FilteringSail) data.sail;
		SailFilter filter = sail.getFilter(null);
		Assert.assertTrue(filter instanceof OpSailFilter);
		OpSailFilter opf = (OpSailFilter) filter;
		Assert.assertSame(opf.getOperation(), OpSailFilter.AND);

		SailFilter filter1 = opf.getFilter1();
		Assert.assertTrue(filter1 instanceof LuceneMatchExprSailFilter);

		SailFilter filter2 = opf.getFilter2();
		Assert.assertTrue(filter2 instanceof OpSailFilter);
		OpSailFilter opf1 = (OpSailFilter) filter2;
		Assert.assertSame(opf1.getOperation(), OpSailFilter.OR);

		SailFilter filter21 = opf1.getFilter1();
		Assert.assertTrue(filter21 instanceof PredicateSailFilter);
		PredicateSailFilter prf1 = (PredicateSailFilter) filter21;
		Assert.assertEquals(prf1.getPredicate(),
				Set.of(data.sail.getValueFactory().createIRI(SailTest.NAMESPACE + "text")));

		SailFilter filter22 = opf1.getFilter2();
		Assert.assertTrue(filter22 instanceof PredicateSailFilter);
		PredicateSailFilter prf2 = (PredicateSailFilter) filter22;
		Assert.assertEquals(prf2.getPredicate(),
				Set.of(data.sail.getValueFactory().createIRI(SailTest.NAMESPACE + "typeof")));

		Sail chain = sail.getOnYesSail();
		assertLuceneSail(chain, SailTest.NAMESPACE + "luceneIndex_fr",
				data.compiler.parseDir("${locationNative}lucene1"), "fr");
		chain = ((LuceneSail) chain).getBaseSail();
		assertLuceneSail(chain, SailTest.NAMESPACE + "luceneIndex_de",
				data.compiler.parseDir("${locationNative}lucene2"), "de");
		chain = ((LuceneSail) chain).getBaseSail();
		assertLuceneSail(chain, SailTest.NAMESPACE + "luceneIndex_es",
				data.compiler.parseDir("${locationNative}lucene3"), "es");
		chain = ((LuceneSail) chain).getBaseSail();
		Assert.assertFalse(chain instanceof LuceneSail);
	}

	@Test
	public void loadModel4Test() throws IOException, SailCompiler.SailCompilerException {
		LoadData data = loadFile("model/model_example4.ttl");

		FilteringSail sail = (FilteringSail) data.sail;
		SailFilter filter = sail.getFilter(null);
		Assert.assertTrue(filter instanceof OpSailFilter);
		OpSailFilter opf = (OpSailFilter) filter;
		Assert.assertSame(opf.getOperation(), OpSailFilter.AND);

		SailFilter filter1 = opf.getFilter1();
		Assert.assertTrue(filter1 instanceof LuceneMatchExprSailFilter);

		SailFilter filter2 = opf.getFilter2();
		Assert.assertTrue(filter2 instanceof OpSailFilter);
		OpSailFilter opf1 = (OpSailFilter) filter2;
		Assert.assertSame(opf1.getOperation(), OpSailFilter.OR);

		SailFilter filter21 = opf1.getFilter1();
		Assert.assertTrue(filter21 instanceof PredicateSailFilter);
		PredicateSailFilter prf1 = (PredicateSailFilter) filter21;
		Assert.assertEquals(prf1.getPredicate(),
				Set.of(data.sail.getValueFactory().createIRI(SailTest.NAMESPACE + "text")));

		SailFilter filter22 = opf1.getFilter2();
		Assert.assertTrue(filter22 instanceof PredicateSailFilter);
		PredicateSailFilter prf2 = (PredicateSailFilter) filter22;
		Assert.assertEquals(prf2.getPredicate(),
				Set.of(data.sail.getValueFactory().createIRI(SailTest.NAMESPACE + "typeof")));

		Assert.assertTrue(sail.getOnYesSail() instanceof MultiTypeFilteringSail);
		MultiTypeFilteringSail multiTypeFilter = (MultiTypeFilteringSail) sail.getOnYesSail();
		Assert.assertEquals(data.sail.getValueFactory().createIRI(SailTest.NAMESPACE + "typeof"),
				multiTypeFilter.getPredicate());
		List<MultiTypeFilteringSail.TypedSail> typedSails = multiTypeFilter.getTypes();
		Assert.assertEquals(2, typedSails.size());
		MultiTypeFilteringSail.TypedSail type1 = typedSails.get(0);
		Assert.assertEquals(List.of(data.sail.getValueFactory().createIRI(SailTest.NAMESPACE + "type1")),
				type1.getType());

		Sail chain = type1.getSail();
		assertLuceneSail(chain, SailTest.NAMESPACE + "luceneIndex_fr_type1",
				data.compiler.parseDir("${locationNative}lucene_fr_type1"), "fr", new LuceneParam().with("wktFields",
						"http://nuts.de/geometry https://linkedopendata.eu/prop/direct/P127"));
		chain = ((LuceneSail) chain).getBaseSail();
		assertLuceneSail(chain, SailTest.NAMESPACE + "luceneIndex_de_type1",
				data.compiler.parseDir("${locationNative}lucene_de_type1"), "de", new LuceneParam().with("wktFields",
						"http://nuts.de/geometry https://linkedopendata.eu/prop/direct/P127"));
		chain = ((LuceneSail) chain).getBaseSail();
		assertLuceneSail(chain, SailTest.NAMESPACE + "luceneIndex_es_type1",
				data.compiler.parseDir("${locationNative}lucene_es_type1"), "es", new LuceneParam().with("wktFields",
						"http://nuts.de/geometry https://linkedopendata.eu/prop/direct/P127"));
		chain = ((LuceneSail) chain).getBaseSail();
		Assert.assertFalse(chain instanceof LuceneSail);

		MultiTypeFilteringSail.TypedSail type2 = typedSails.get(1);
		Assert.assertEquals(List.of(data.sail.getValueFactory().createIRI(SailTest.NAMESPACE + "type2")),
				type2.getType());

		chain = type2.getSail();
		assertLuceneSail(chain, SailTest.NAMESPACE + "luceneIndex_fr_type2",
				data.compiler.parseDir("${locationNative}lucene_fr_type2"), "fr", new LuceneParam().with("wktFields",
						"http://nuts.de/geometry https://linkedopendata.eu/prop/direct/P127"));
		chain = ((LuceneSail) chain).getBaseSail();
		assertLuceneSail(chain, SailTest.NAMESPACE + "luceneIndex_de_type2",
				data.compiler.parseDir("${locationNative}lucene_de_type2"), "de", new LuceneParam().with("wktFields",
						"http://nuts.de/geometry https://linkedopendata.eu/prop/direct/P127"));
		chain = ((LuceneSail) chain).getBaseSail();
		assertLuceneSail(chain, SailTest.NAMESPACE + "luceneIndex_es_type2",
				data.compiler.parseDir("${locationNative}lucene_es_type2"), "es", new LuceneParam().with("wktFields",
						"http://nuts.de/geometry https://linkedopendata.eu/prop/direct/P127"));
		chain = ((LuceneSail) chain).getBaseSail();
		Assert.assertFalse(chain instanceof LuceneSail);
	}

	@Test
	public void loadModel5Test() throws IOException, SailCompiler.SailCompilerException {
		LoadData data = loadFile("model/model_example5.ttl");

		FilteringSail sail = (FilteringSail) data.sail;
		SailFilter filter = sail.getFilter(null);
		Assert.assertTrue(filter instanceof OpSailFilter);
		OpSailFilter opf = (OpSailFilter) filter;
		Assert.assertSame(opf.getOperation(), OpSailFilter.AND);

		SailFilter filter1 = opf.getFilter1();
		Assert.assertTrue(filter1 instanceof LuceneMatchExprSailFilter);

		SailFilter filter2 = opf.getFilter2();
		Assert.assertTrue(filter2 instanceof OpSailFilter);
		OpSailFilter opf1 = (OpSailFilter) filter2;
		Assert.assertSame(opf1.getOperation(), OpSailFilter.OR);

		SailFilter filter21 = opf1.getFilter1();
		Assert.assertTrue(filter21 instanceof PredicateSailFilter);
		PredicateSailFilter prf1 = (PredicateSailFilter) filter21;
		Assert.assertEquals(prf1.getPredicate(),
				Set.of(data.sail.getValueFactory().createIRI(SailTest.NAMESPACE + "text")));

		SailFilter filter22 = opf1.getFilter2();
		Assert.assertTrue(filter22 instanceof PredicateSailFilter);
		PredicateSailFilter prf2 = (PredicateSailFilter) filter22;
		Assert.assertEquals(prf2.getPredicate(),
				Set.of(data.sail.getValueFactory().createIRI(SailTest.NAMESPACE + "typeof")));

		Assert.assertTrue(sail.getOnYesSail() instanceof MultiTypeFilteringSail);
		MultiTypeFilteringSail multiTypeFilter = (MultiTypeFilteringSail) sail.getOnYesSail();
		Assert.assertEquals(data.sail.getValueFactory().createIRI(SailTest.NAMESPACE + "typeof"),
				multiTypeFilter.getPredicate());
		List<MultiTypeFilteringSail.TypedSail> typedSails = multiTypeFilter.getTypes();
		Assert.assertEquals(2, typedSails.size());
		MultiTypeFilteringSail.TypedSail type1 = typedSails.get(0);
		Assert.assertEquals(List.of(data.sail.getValueFactory().createIRI(SailTest.NAMESPACE + "type1"),
				data.sail.getValueFactory().createIRI(SailTest.NAMESPACE + "type3"),
				data.sail.getValueFactory().createIRI(SailTest.NAMESPACE + "type4")), type1.getType());

		Sail chain = type1.getSail();
		assertLuceneSail(chain, SailTest.NAMESPACE + "luceneIndex_fr_type1",
				data.compiler.parseDir("${locationNative}lucene_fr_type1"), "fr en ro", new LuceneParam()
						.with("wktFields", "http://nuts.de/geometry https://linkedopendata.eu/prop/direct/P127"));
		chain = ((LuceneSail) chain).getBaseSail();
		assertLuceneSail(chain, SailTest.NAMESPACE + "luceneIndex_de_type1",
				data.compiler.parseDir("${locationNative}lucene_de_type1"), "de lv bg", new LuceneParam()
						.with("wktFields", "http://nuts.de/geometry https://linkedopendata.eu/prop/direct/P127"));
		chain = ((LuceneSail) chain).getBaseSail();
		assertLuceneSail(chain, SailTest.NAMESPACE + "luceneIndex_es_type1",
				data.compiler.parseDir("${locationNative}lucene_es_type1"), "es", new LuceneParam().with("wktFields",
						"http://nuts.de/geometry https://linkedopendata.eu/prop/direct/P127"));
		chain = ((LuceneSail) chain).getBaseSail();
		Assert.assertFalse(chain instanceof LuceneSail);

		MultiTypeFilteringSail.TypedSail type2 = typedSails.get(1);
		Assert.assertEquals(List.of(data.sail.getValueFactory().createIRI(SailTest.NAMESPACE + "type2"),
				data.sail.getValueFactory().createIRI(SailTest.NAMESPACE + "type5"),
				data.sail.getValueFactory().createIRI(SailTest.NAMESPACE + "type6")), type2.getType());

		chain = type2.getSail();
		assertLuceneSail(chain, SailTest.NAMESPACE + "luceneIndex_fr_type2",
				data.compiler.parseDir("${locationNative}lucene_fr_type2"), "fr en ro", new LuceneParam()
						.with("wktFields", "http://nuts.de/geometry https://linkedopendata.eu/prop/direct/P127"));
		chain = ((LuceneSail) chain).getBaseSail();
		assertLuceneSail(chain, SailTest.NAMESPACE + "luceneIndex_de_type2",
				data.compiler.parseDir("${locationNative}lucene_de_type2"), "de lv bg", new LuceneParam()
						.with("wktFields", "http://nuts.de/geometry https://linkedopendata.eu/prop/direct/P127"));
		chain = ((LuceneSail) chain).getBaseSail();
		assertLuceneSail(chain, SailTest.NAMESPACE + "luceneIndex_es_type2",
				data.compiler.parseDir("${locationNative}lucene_es_type2"), "es", new LuceneParam().with("wktFields",
						"http://nuts.de/geometry https://linkedopendata.eu/prop/direct/P127"));
		chain = ((LuceneSail) chain).getBaseSail();
		Assert.assertFalse(chain instanceof LuceneSail);
	}

	@Test
	public void dirCompileTest() throws SailCompiler.SailCompilerException {
		SailCompiler compiler = new SailCompiler();
		compiler.registerDirString("myKey", "my cat");
		Assert.assertEquals("Dir string not parsed", "I love my cat", compiler.parseDir("I love ${myKey}"));
	}

	@Test(expected = SailCompiler.SailCompilerException.class)
	public void dirCompileNoKeyFailTest() throws SailCompiler.SailCompilerException {
		SailCompiler compiler = new SailCompiler();
		compiler.parseDir("I love ${myKey}");
	}

	@Test
	public void parsedStringTest() throws SailCompiler.SailCompilerException, IOException {
		SailCompiler compiler = new SailCompiler();

		try (InputStream is = getClass().getClassLoader().getResourceAsStream("model/model_param.ttl")) {
			compiler.load(is, Rio.getParserFormatForFileName("model/model_param.ttl").orElseThrow());
		}

		MemoryStore source = new MemoryStore();
		compiler.compile(source);
		Assert.assertEquals("aaa my value bbb my value 2", compiler.asLitString(SimpleValueFactory.getInstance()
				.createLiteral("aaa ${test} bbb ${test2}", SailCompilerSchema.PARSED_STRING_DATATYPE)));
	}

	@Test
	public void objectParsingTest() {
		SailCompiler compiler = new SailCompiler();
		compiler.registerDirObject(new ParsedStringObject("testLocation"));
		Assert.assertEquals("testLocation", compiler.parseDir("${locationNative}"));

	}

	private static class LoadData {
		NotifyingSail sail;
		SailCompiler compiler;
		String nativeDirectory;

		public LoadData(NotifyingSail sail, SailCompiler compiler, String nativeDirectory) {
			this.sail = sail;
			this.compiler = compiler;
			this.nativeDirectory = nativeDirectory;
		}
	}

	private static class LuceneParam {
		private final Map<String, String> map = new HashMap<>();

		public LuceneParam with(String key, String value) {
			map.put(key, value);
			return this;
		}

		public void assertParams(LuceneSail sail) {
			map.forEach((key, value) -> Assert.assertEquals(value, sail.getParameter(key)));
		}
	}

	private static class ParsedStringObject {
		private final String locationNative;

		public ParsedStringObject(String locationNative) {
			this.locationNative = locationNative;
		}

		@ParsedStringValue("locationNative")
		public String getLocationNative() {
			return locationNative;
		}

	}
}
