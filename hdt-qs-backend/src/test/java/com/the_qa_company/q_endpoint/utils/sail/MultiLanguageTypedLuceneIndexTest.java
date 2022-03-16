package com.the_qa_company.q_endpoint.utils.sail;

import com.the_qa_company.q_endpoint.hybridstore.HybridStore;
import com.the_qa_company.q_endpoint.utils.sail.filter.PredicateSailFilter;
import com.the_qa_company.q_endpoint.utils.sail.helpers.LuceneSailBuilder;
import com.the_qa_company.q_endpoint.utils.sail.linked.SimpleLinkedSail;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.evaluation.TupleFunctionEvaluationMode;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MultiLanguageTypedLuceneIndexTest extends SailTest {
	private final String[] languages = {
			"fr",
			"en",
			"de",
			"es"
	};

	private final String[] types = {
			"type1",
			"type2",
			"type3"
	};

	@Override
	protected Sail configStore(HybridStore hybridStore) {
		// the multiple language to link
		// directory to store the index
		String dir = hybridStore.getHybridStoreFiles().getLocationNative() + "lucene";
		// a filter to filter ex:text and ex:typeof
		return new FilteringSail(
				// a filter to filter the type of the subjects
				new MultiTypeFilteringSail(
						// type IRI to define the type
						iri("typeof"),
						Arrays.stream(types).map(type ->
								// redirection of the type ex:type1
								new MultiTypeFilteringSail.TypedSail(
										// link each language sails
										SimpleLinkedSail.linkSails(
												Arrays.stream(languages).map(language ->
														new LuceneSailBuilder()
																.withDir(dir + type + "-" + language)
																.withId(NAMESPACE + "lucene" + type + "_" + language)
																.withLanguageFiltering(language)
																.withEvaluationMode(TupleFunctionEvaluationMode.NATIVE)
																.build()
												),
												LuceneSail::setBaseSail
										),
										iri(type)
								)).collect(Collectors.toList())
				),
				hybridStore,
				new PredicateSailFilter(iri("text"))
						.or(new PredicateSailFilter(iri("typeof")))
		);
	}

	private void injectTriples() {
		int index = 0;
		try (SailRepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			for (String type : types) {
				for (String lang : languages) {
					connection.add(
							VF.createStatement(
									iri("subj" + index),
									iri("typeof"),
									iri(type)
							)
					);
					connection.add(
							VF.createStatement(
									iri("subj" + index),
									iri("text"),
									VF.createLiteral(type + lang + index, lang)
							)
					);
					index++;
				}
			}
			connection.commit();
		}

		logger.debug("add {} subjects to test, with {} types and {} languages", index, types.length, languages.length);
	}

	@Test
	public void multiLanguageTypedIndexTest() {
		injectTriples();

		int index = 0;

		for (String type : types) {
			for (String lang : languages) {
				for (String type2 : types) {
					for (String lang2 : languages) {
						if (type2.equals(type) && lang.equals(lang2)) {
							continue;
						}

						assertSelect(
								new LuceneSelectWhereBuilder("r", type + lang + index)
										.withIndexId("ex:lucene" + type2 + "_" + lang2)
										.buildWithSelectWhereClause()
						);
					}
				}

				assertSelect(
						new LuceneSelectWhereBuilder("r", type + lang + index)
								.withIndexId("ex:lucene" + type + "_" + lang)
								.buildWithSelectWhereClause(),
						new SelectResultRow().withValue("r", iri("subj" + index))
				);
				index++;
			}
		}

	}

	@Test
	public void multiLanguageTypedIndexOneRequestTest() {
		injectTriples();

		int index = 0;

		List<String> queries = new ArrayList<>();
		List<SelectResultRow> rows = new ArrayList<>();

		for (String type : types) {
			for (String lang : languages) {
				queries.add(
						new LuceneSelectWhereBuilder("r" + index, type + lang + index)
								.withIndexId("ex:lucene" + type + "_" + lang)
								.build()
				);
				rows.add(
						new SelectResultRow()
								.withValue("r" + index, iri("subj" + index))
				);
				index++;
			}
		}

		assertSelect(
				joinLines(
						"SELECT * {",
						queries.stream().map(q -> "{ " + q + " }").collect(Collectors.joining("UNION")),
						"}"
				),
				rows.toArray(SelectResultRow[]::new));

	}
}
