package com.the_qa_company.q_endpoint.utils.sail;

import com.the_qa_company.q_endpoint.HybridStoreTest;
import com.the_qa_company.q_endpoint.hybridstore.HybridStore;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rdfhdt.hdt.dictionary.DictionaryFactory;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
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
	private static final String PREFIXES = joinLines(
			"PREFIX search: <http://www.openrdf.org/contrib/lucenesail#>",
			"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
			"PREFIX ex: <" + NAMESPACE + ">");
	/**
	 * basic SPO query "SELECT * {?s ?p ?o}"
	 */
	public static final String SPO_QUERY = "SELECT * {?s ?p ?o}";
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
	 * join lines with \n
	 *
	 * @param lines the lines
	 * @return the joined lines
	 */
	public static String joinLines(String... lines) {
		return String.join("\n", lines);
	}

	/**
	 * convert a value (IRI or literal) to a NT string representation
	 * @param value the value
	 * @return string
	 */
	public static String asNT(Value value) {
		if (value.isLiteral()) {
			Literal lit = (Literal) value;
			CoreDatatype type = lit.getCoreDatatype();
			if (type == CoreDatatype.RDF.LANGSTRING) {
				return "'" + lit.getLabel() + "'" + lit.getLanguage().map(l -> "@" + l).orElse("");
			} else if (type != CoreDatatype.NONE) {
				return "'" + lit.getLabel() + "'^^<" + type.getIri() + ">";
			} else {
				return "'" + lit.getLabel() + "'";
			}
		}

		if (value.isIRI()) {
			return "<" + value + ">";
		}
		throw new RuntimeException("Can't convert " + value + " to ttl");
	}

	/**
	 * convert a statement of IRI or literal to a NT string representation
	 * @param statement the statement
	 * @return string
	 */
	public static String asNT(Statement statement) {
		return asNT(statement.getSubject()) + " "
				+ asNT(statement.getPredicate()) + " "
				+ asNT(statement.getObject());
	}

	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	protected SailRepository repository;
	protected HDTSpecification spec;

	@Before
	public void setup() throws IOException {
		spec = new HDTSpecification();
		spec.set("tempDictionary.impl", DictionaryFactory.MOD_DICT_IMPL_MULT_HASH);
		spec.set("dictionary.type", DictionaryFactory.DICTIONARY_TYPE_MULTI_OBJECTS);
		File nativeStore = tempDir.newFolder("native-store");
		File hdtStore = tempDir.newFolder("hdt-store");

		configHDT(hdtStore.getAbsolutePath() + "/" + HybridStoreTest.HDT_INDEX_NAME);

		HybridStore hybridStore = new HybridStore(
				hdtStore.getAbsolutePath() + "/", HybridStoreTest.HDT_INDEX_NAME, spec,
				nativeStore.getAbsolutePath() + "/", true
		);

		Sail store = configStore(hybridStore);

		repository = new SailRepository(store);
		repository.init();
	}

	/**
	 * override to define the HDT, by default create an empty HDT
	 *
	 * @param indexLocation the wanted hdt file location
	 * @throws IOException io exception
	 */
	protected void configHDT(String indexLocation) throws IOException {
		HDT hdt = com.the_qa_company.q_endpoint.Utility.createTempHdtIndex(tempDir, true, false, spec);
		assert hdt != null;
		hdt.saveToHDT(indexLocation, null);
	}

	/**
	 * define the sail with the hybridstore
	 *
	 * @param hybridStore the store to use as a triple source
	 * @return sail
	 */
	protected abstract Sail configStore(HybridStore hybridStore);

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
			List<SelectResultRow> rows = Arrays.stream(exceptedElements).collect(Collectors.toCollection(ArrayList::new));

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
					Assert.fail(
							"Can't find a row for the result: " + set + "\n"
									+ rows.stream().map(SelectResultRow::toString).collect(Collectors.joining("\n"))
					);
				}

				// missing rows?
				Assert.assertTrue(
						"Missing rows: \n"
								+ rows.stream().map(SelectResultRow::toString).collect(Collectors.joining("\n")),
						rows.isEmpty()
				);
			}
		}
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

		add(
				stmt1,
				stmt2,
				stmt3
		);

		assertSelect(
				SPO_QUERY,
				new SelectResultRow().withSPO(stmt1),
				new SelectResultRow().withSPO(stmt2),
				new SelectResultRow().withSPO(stmt3)
		);

		remove(
				stmt1
		);

		printValues();

		assertSelect(
				SPO_QUERY,
				new SelectResultRow().withSPO(stmt2),
				new SelectResultRow().withSPO(stmt3)
		);

		remove(
				stmt2,
				stmt3
		);

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

		public SelectResultRow() {}

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
		 * define an excepted literal for this object name
		 *
		 * @param object the object name
		 * @param value  the excepted literal value
		 * @return this
		 */
		public SelectResultRow withLiteral(String object, String value) {
			return withValue(object, VF.createLiteral(value));
		}

		/**
		 * define an excepted literal for this object name
		 *
		 * @param object   the object name
		 * @param value    the excepted literal value
		 * @param language the excepted language for the literal
		 * @return this
		 */
		public SelectResultRow withLiteral(String object, String value, String language) {
			return withValue(object, VF.createLiteral(value, language));
		}

		/**
		 * add the object (s, p, o) with the elements of a statement
		 *
		 * @param statement the statement
		 * @return this
		 */
		public SelectResultRow withSPO(Statement statement) {
			return withValue("s", statement.getSubject())
					.withValue("p", statement.getPredicate())
					.withValue("o", statement.getObject());
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
			return values.entrySet().stream()
					.map(e -> "\"" + e.getKey() + "\": " + e.getValue())
					.collect(Collectors.joining(", ", "{", "}"));
		}
	}

	/**
	 * Class to create a Lucene SELECT WHERE clause
	 * @author Antoine Willerval
	 */
	protected static class LuceneSelectWhereBuilder {
		private final String resultObject;
		private final String query;
		private String indexId;
		private String property;
		private String scoreObject;
		private String snippetObject;

		/**
		 * create a lucene select where builder
		 * @param resultObject the subject of the query
		 * @param query the text query
		 */
		public LuceneSelectWhereBuilder(String resultObject, String query) {
			this.resultObject = resultObject;
			this.query = query;
		}

		/**
		 * set search:indexid
		 * @param indexId the index id
		 * @return this
		 */
		public LuceneSelectWhereBuilder withIndexId(String indexId) {
			this.indexId = indexId;
			return this;
		}

		/**
		 * set search:property
		 * @param property the property
		 * @return this
		 */
		public LuceneSelectWhereBuilder withProperty(String property) {
			this.property = property;
			return this;
		}

		/**
		 * set search:score
		 * @param scoreObject the score object
		 * @return this
		 */
		public LuceneSelectWhereBuilder withScoreObject(String scoreObject) {
			this.scoreObject = scoreObject;
			return this;
		}

		/**
		 * set search:snippet
		 * @param snippetObject the snippet object
		 * @return this
		 */
		public LuceneSelectWhereBuilder withSnippetObject(String snippetObject) {
			this.snippetObject = snippetObject;
			return this;
		}

		/**
		 * build the query
		 * @return query
		 */
		public String build() {
			String s =
					"?" + resultObject + " search:matches [\n"
							+ "  search:query '" + query.replaceAll("'", "\\'") + "';\n";

			if (indexId != null) {
				s += "  search:indexid " + indexId + " ;\n";
			}
			if (property != null) {
				s += "  search:property " + property + " ;\n";
			}

			if (scoreObject != null) {
				s += "  search:score ?" + scoreObject + " ;\n";
			}
			if (snippetObject != null) {
				s += "  search:snippet ?" + snippetObject + " ;\n";
			}

			return s + "].";
		}
	}
}
