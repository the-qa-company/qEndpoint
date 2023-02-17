package com.the_qa_company.qendpoint.utils.sail;

import com.the_qa_company.qendpoint.store.EndpointStore;
import com.the_qa_company.qendpoint.store.MergeRunnable;
import com.the_qa_company.qendpoint.store.MergeRunnableStopPoint;
import com.the_qa_company.qendpoint.utils.sail.filter.LuceneMatchExprSailFilter;
import com.the_qa_company.qendpoint.utils.sail.filter.PredicateSailFilter;
import com.the_qa_company.qendpoint.utils.sail.helpers.LuceneSailBuilder;
import com.the_qa_company.qendpoint.utils.sail.linked.SimpleLinkedSail;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.evaluation.TupleFunctionEvaluationMode;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RunWith(Parameterized.class)
public class MultiLanguageTypedLuceneIndexTest extends SailTest {
	private static final List<String> languages = IntStream.range(0, 2)
			.mapToObj(MultiLanguageTypedLuceneIndexTest::stringNameOfInt).collect(Collectors.toList());
	private static final List<String> types = IntStream.range(0, 2)
			.mapToObj(MultiLanguageTypedLuceneIndexTest::stringNameOfInt).collect(Collectors.toList());

	/**
	 * create a lowercase name from a number, to create string without any
	 * number in it
	 *
	 * @param i id
	 * @return string
	 */
	public static String stringNameOfInt(int i) {
		String table = "abcdefghijklmnopqrstuvwxyz";
		StringBuilder out = new StringBuilder();
		int c = i;
		do {
			out.append(table.charAt(c % table.length()));
			c /= table.length();
		} while (c != 0);
		return out.toString();
	}

	@Parameterized.Parameters(name = "WithMerge {0}")
	public static Collection<Object> params() {
		return Arrays.asList(false, true);
	}

	private final boolean mergeBefore;

	public MultiLanguageTypedLuceneIndexTest(boolean mergeBefore) {
		this.mergeBefore = mergeBefore;
	}

	@Override
	protected Sail configStore(EndpointStore endpoint) {
		// the multiple language to link
		// directory to store the index
		String dir = endpoint.getEndpointFiles().getLocationNative() + "lucene";
		// a filter to filter ex:text and ex:typeof
		return new FilteringSail(
				// a filter to filter the type of the subjects
				new MultiTypeFilteringSail(
						// type IRI to define the type
						iri("typeof"), types.stream().map(type ->
						// redirection of the type ex:type1
						new MultiTypeFilteringSail.TypedSail(
								// link each language sails
								SimpleLinkedSail.linkSails(languages.stream()
										.map(language -> new LuceneSailBuilder().withDir(dir + type + "-" + language)
												.withId(NAMESPACE + "lucene" + type + "_" + language)
												.withLanguageFiltering(language)
												.withEvaluationMode(TupleFunctionEvaluationMode.NATIVE).build()),
										LuceneSail::setBaseSail),
								iri(type))).collect(Collectors.toList())),
				endpoint, new PredicateSailFilter(iri("text")).or(new PredicateSailFilter(iri("typeof")))
						.and(new LuceneMatchExprSailFilter()));
	}

	private void injectTriples() {
		int index = 0;
		try (SailRepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			for (String type : types) {
				for (String lang : languages) {
					connection.add(VF.createStatement(iri("subj" + index), iri("typeof"), iri(type)));
					connection.add(VF.createStatement(iri("subj" + index), iri("text"),
							VF.createLiteral(type + lang + index, lang)));
					index++;
				}
			}
			connection.commit();
		}

		logger.info("added {} subjects to test, with {} types and {} languages in {}", index, types.size(),
				languages.size(), watch.stopAndShow());
		watch.reset();

		if (mergeBefore) {
			MergeRunnableStopPoint.debug = true;

			endpoint.mergeStore();

			try {
				MergeRunnable.debugWaitMerge();
			} catch (InterruptedException e) {
				throw new AssertionError("Can't wait merge", e);
			}
			MergeRunnableStopPoint.debug = false;

		}
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

						assertSelect(new LuceneSelectWhereBuilder("r", type + lang + index)
								.withIndexId("ex:lucene" + type2 + "_" + lang2).buildWithSelectWhereClause());
					}
				}

				assertSelect(
						new LuceneSelectWhereBuilder("r", type + lang + index)
								.withIndexId("ex:lucene" + type + "_" + lang).buildWithSelectWhereClause(),
						new SelectResultRow().withValue("r", iri("subj" + index)));
				index++;
			}
		}
	}

	@Test
	public void multiLanguageTypedIndexOneRequestUnionTest() {
		injectTriples();
		int index = 0;

		List<String> queries = new ArrayList<>();
		List<SelectResultRow> rows = new ArrayList<>();

		for (String type : types) {
			for (String lang : languages) {
				queries.add(new LuceneSelectWhereBuilder("r" + index, type + lang + index)
						.withIndexId("ex:lucene" + type + "_" + lang).build());
				rows.add(new SelectResultRow().withValue("r" + index, iri("subj" + index)));
				index++;
			}
		}

		assertSelect(joinLines("SELECT * {",
				queries.stream().map(q -> "{ " + q + " }").collect(Collectors.joining("UNION")), "}"),
				rows.toArray(SelectResultRow[]::new));

	}

	@Test
	public void multiLanguageTypedIndexOneRequestJoinTest() {
		injectTriples();
		int index = 0;

		List<String> queries = new ArrayList<>();
		SelectResultRow row = new SelectResultRow();

		for (String type : types) {
			for (String lang : languages) {
				queries.add(new LuceneSelectWhereBuilder("r" + index, type + lang + index)
						.withIndexId("ex:lucene" + type + "_" + lang).build());
				row.withValue("r" + index, iri("subj" + index));
				index++;
			}
		}

		assertSelect(joinLines("SELECT * {", String.join("\n", queries), "}"), row);

	}
}
