package com.the_qa_company.qendpoint.utils.sail;

import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import com.the_qa_company.qendpoint.store.EndpointStore;
import com.the_qa_company.qendpoint.store.EndpointStoreTest;
import com.the_qa_company.qendpoint.store.EndpointStoreUtils;
import com.the_qa_company.qendpoint.store.MergeRunnableStopPoint;
import com.the_qa_company.qendpoint.store.Utility;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Class to test sail implementations
 *
 * @author Antoine Willerval
 */
public abstract class SailTest {
	/**
	 * ex: namespace
	 */
	public static final String NAMESPACE = "http://example.org/";
	/**
	 * basic prefixes, search:, rdfs: and ex:
	 */
	public static final String PREFIXES = joinLines("PREFIX search: <http://www.openrdf.org/contrib/lucenesail#>",
			"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>", "PREFIX ex: <" + NAMESPACE + ">");
	/**
	 * basic SPO query "SELECT * {?s ?p ?o}"
	 */
	public static final String SPO_QUERY = "SELECT * {?s ?p ?o}";

	public static final boolean useMultiSect = true;
	/**
	 * Value factory
	 */
	protected static final ValueFactory VF = SimpleValueFactory.getInstance();

	/**
	 * create ex: iri
	 *
	 * @param name the iri name
	 * @return iri
	 */
	protected static IRI iri(String name) {
		return VF.createIRI(NAMESPACE + name);
	}

	/**
	 * create a statement ex:MOCKS+s ex:MOCKS+p "o"^^xsd:integer
	 *
	 * @param s s
	 * @param p p
	 * @param o o
	 * @return statement
	 */
	protected static Statement mockStmt(long s, long p, long o) {
		return VF.createStatement(iri("MOCKS" + s), iri("MOCKP" + p), VF.createLiteral(o));
	}

	/**
	 * join lines with \n
	 *
	 * @param lines the lines
	 * @return the joined lines
	 */
	public static String joinLines(String... lines) {
		return String.join("\n", lines);
	}

	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	protected SailRepository repository;
	protected HDTOptions spec;
	protected StopWatch watch;
	protected EndpointStore endpoint;
	private int addCount = 0;
	private int removeCount = 0;
	private int selectCount = 0;

	@Before
	public void setup() throws IOException {
		EndpointStoreUtils.enableDebugConnection();
		watch = new StopWatch();
		addCount = 0;
		removeCount = 0;
		selectCount = 0;
		spec = HDTOptions.of();
		if (useMultiSect) {
			spec.set(HDTOptionsKeys.TEMP_DICTIONARY_IMPL_KEY, HDTOptionsKeys.TEMP_DICTIONARY_IMPL_VALUE_MULT_HASH);
			spec.set(HDTOptionsKeys.DICTIONARY_TYPE_KEY, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS);
		}
		Path nativeStore = tempDir.newFolder("native-store").toPath();
		Path hdtStore = tempDir.newFolder("hdt-store").toPath();

		configHDT(hdtStore.resolve(EndpointStoreTest.HDT_INDEX_NAME));

		endpoint = new EndpointStore(hdtStore, EndpointStoreTest.HDT_INDEX_NAME, spec, nativeStore, false);

		Sail store = configStore(endpoint);

		repository = new SailRepository(store);
		repository.init();
		logger.info("Setup the repository in {}", watch.stopAndShow());
		watch.reset();
	}

	@After
	public void complete() {
		// unlock points if required
		MergeRunnableStopPoint.debug = false;
		logger.info("Unlock merge points");
		MergeRunnableStopPoint.unlockAll();

		logger.info("Completed S/A/R : {}/{}/{} in {}", selectCount, addCount, removeCount, watch.stopAndShow());
		repository.shutDown();

		EndpointStoreUtils.disableDebugConnection();
	}

	/**
	 * override to define the HDT, by default create an empty HDT
	 *
	 * @param indexLocation the wanted hdt file location
	 * @throws IOException io exception
	 */
	protected void configHDT(Path indexLocation) throws IOException {
		try (HDT hdt = Utility.createTempHdtIndex(tempDir, true, false, spec)) {
			assert hdt != null;
			hdt.saveToHDT(indexLocation.toAbsolutePath().toString(), null);
		}
	}

	/**
	 * define the sail with the store
	 *
	 * @param endpoint the store to use as a triple source
	 * @return sail
	 */
	protected Sail configStore(EndpointStore endpoint) {
		return endpoint;
	}

	/**
	 * add statements to the repository
	 *
	 * @param statements the statements
	 */
	protected void add(Statement... statements) {
		try (SailRepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			for (Statement statement : statements) {
				connection.add(statement);
			}
			connection.commit();
		}
		addCount += statements.length;
	}

	/**
	 * remove statements to the repository
	 *
	 * @param statements the statements
	 */
	protected void remove(Statement... statements) {
		try (SailRepositoryConnection connection = repository.getConnection()) {
			for (Statement statement : statements) {
				connection.remove(statement);
			}
		}
		removeCount += statements.length;
	}

	/**
	 * make a SELECT and assert the results
	 *
	 * @param query            the query to select
	 * @param exceptedElements the results to assert
	 */
	protected void assertSelect(String query, SelectResultRow... exceptedElements) {
		try (SailRepositoryConnection connection = repository.getConnection()) {
			// add prefixes
			query = PREFIXES + "\n" + query;

			// the rows to find
			List<SelectResultRow> rows = Arrays.stream(exceptedElements)
					.collect(Collectors.toCollection(ArrayList::new));

			// prepare the query
			TupleQuery preparedQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query, NAMESPACE);
			// evaluate the query
			try (TupleQueryResult result = preparedQuery.evaluate()) {
				resultNext:
				while (result.hasNext()) {
					BindingSet set = result.next();
					Iterator<SelectResultRow> it = rows.iterator();
					// searching a matching row
					while (it.hasNext()) {
						SelectResultRow row = it.next();
						if (row.match(set)) {
							it.remove();
							continue resultNext;
						}
					}
					// no matching row for the current BindingSet
					Assert.fail("Can't find a row for the result: " + set + "\n"
							+ rows.stream().map(SelectResultRow::toString).collect(Collectors.joining("\n")));
				}

				// missing rows?
				Assert.assertTrue(
						"Missing rows: \n"
								+ rows.stream().map(SelectResultRow::toString).collect(Collectors.joining("\n")),
						rows.isEmpty());
			}
		}
		++selectCount;
	}

	/**
	 * print the values of the repository
	 */
	protected void printValues() {
		try (SailRepositoryConnection connection = repository.getConnection()) {
			try (RepositoryResult<Statement> it = connection.getStatements(null, null, null)) {
				it.forEach(s -> logger.debug("- {}", s));
			}
		}
	}

	@Test
	public void noConfigTest() {
		Statement stmt1 = VF.createStatement(iri("a"), iri("p"), VF.createLiteral("my lit"));
		Statement stmt2 = VF.createStatement(iri("a"), iri("p"), VF.createLiteral("my lit1"));
		Statement stmt3 = VF.createStatement(iri("a"), iri("p"), VF.createLiteral("my lit2"));

		assertSelect(SPO_QUERY);

		add(stmt1, stmt2, stmt3);

		logger.info("added 3 statement to test in {}", watch.stopAndShow());
		watch.reset();

		assertSelect(SPO_QUERY, new SelectResultRow().withSPO(stmt1), new SelectResultRow().withSPO(stmt2),
				new SelectResultRow().withSPO(stmt3));

		remove(stmt1);

		printValues();

		assertSelect(SPO_QUERY, new SelectResultRow().withSPO(stmt2), new SelectResultRow().withSPO(stmt3));

		remove(stmt2, stmt3);

		assertSelect(SPO_QUERY);

		printValues();
	}

	/**
	 * define a row for a result
	 *
	 * @author Antoine Willerval
	 */
	protected static class SelectResultRow {
		private final Map<String, Value> values = new HashMap<>();

		public SelectResultRow() {
		}

		/**
		 * define an excepted value for this object name
		 *
		 * @param object the object name
		 * @param value  the excepted value
		 * @return this
		 */
		public SelectResultRow withValue(String object, Value value) {
			Assert.assertNull("withValue called with the same object twice!", values.put(object, value));
			return this;
		}

		/**
		 * define an excepted IRI for this object name
		 *
		 * @param object  the object name
		 * @param iriName the excepted IRI name (the IRI = ex:$iriName)
		 * @return this
		 */
		public SelectResultRow withIRI(String object, String iriName) {
			return withValue(object, iri(iriName));
		}

		/**
		 * define an excepted IRI for this object name
		 *
		 * @param object the object name
		 * @param iri    the excepted IRI
		 * @return this
		 */
		public SelectResultRow withIRI(String object, IRI iri) {
			return withValue(object, iri);
		}

		/**
		 * add the object (s, p, o) with the elements of a statement
		 *
		 * @param statement the statement
		 * @return this
		 */
		public SelectResultRow withSPO(Statement statement) {
			return withValue("s", statement.getSubject()).withValue("p", statement.getPredicate()).withValue("o",
					statement.getObject());
		}

		/**
		 * test if this row match a {@link org.eclipse.rdf4j.query.BindingSet}
		 *
		 * @param set the set
		 * @return true if this row match the set, false otherwise
		 */
		public boolean match(BindingSet set) {
			Set<String> names = set.getBindingNames();
			for (String key : names) {
				Value value = values.get(key);
				if (value == null) {
					return false;
				}

				Value value2 = set.getValue(key);

				assert value2 != null : "Value2 shouldn't be null";

				if (!value.equals(value2)) {
					return false;
				}
			}
			// check we have the same size
			return names.size() == values.keySet().size();
		}

		@Override
		public String toString() {
			return values.entrySet().stream().map(e -> "\"" + e.getKey() + "\": " + e.getValue())
					.collect(Collectors.joining(", ", "{", "}"));
		}
	}

	/**
	 * Class to create a Lucene SELECT WHERE clause
	 *
	 * @author Antoine Willerval
	 */
	protected static class LuceneSelectWhereBuilder {
		private final String resultObject;
		private final String query;
		private String indexId;
		private String property;

		/**
		 * create a lucene select where builder
		 *
		 * @param resultObject the subject of the query
		 * @param query        the text query
		 */
		public LuceneSelectWhereBuilder(String resultObject, String query) {
			this.resultObject = resultObject;
			this.query = query;
		}

		/**
		 * set search:indexid
		 *
		 * @param indexId the index id
		 * @return this
		 */
		public LuceneSelectWhereBuilder withIndexId(String indexId) {
			this.indexId = indexId;
			return this;
		}

		/**
		 * set search:property
		 *
		 * @param property the property
		 * @return this
		 */
		public LuceneSelectWhereBuilder withProperty(String property) {
			this.property = property;
			return this;
		}

		/**
		 * build the query
		 *
		 * @return query
		 */
		public String build() {
			String s = "?" + resultObject + " search:matches [\n" + "  search:query '" + query.replaceAll("'", "'")
					+ "';\n";

			if (indexId != null) {
				s += "  search:indexid " + indexId + " ;\n";
			}
			if (property != null) {
				s += "  search:property " + property + " ;\n";
			}

			return s + "].";
		}

		/**
		 * build the query with a SELECT (objects) clause
		 *
		 * @param objects the object to get, empty for *
		 * @return query
		 */
		public String buildWithSelectWhereClause(String... objects) {
			String objectToGet;

			if (objects.length == 0) {
				objectToGet = "*";
			} else {
				objectToGet = Arrays.stream(objects).map(s -> "?" + s).collect(Collectors.joining(" "));
			}
			return joinLines("SELECT " + objectToGet + " {", build(), "}");
		}
	}
}
