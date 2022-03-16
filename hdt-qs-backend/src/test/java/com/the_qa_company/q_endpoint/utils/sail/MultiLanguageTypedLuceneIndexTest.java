package com.the_qa_company.q_endpoint.utils.sail;

import com.the_qa_company.q_endpoint.hybridstore.HybridStore;
import com.the_qa_company.q_endpoint.utils.sail.filter.PredicateSailFilter;
import com.the_qa_company.q_endpoint.utils.sail.helpers.LuceneSailBuilder;
import com.the_qa_company.q_endpoint.utils.sail.linked.SimpleLinkedSail;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.evaluation.TupleFunctionEvaluationMode;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;
import org.junit.Test;

import java.util.Arrays;

public class MultiLanguageTypedLuceneIndexTest extends SailTest {
	private final String[] languages = {
			"fr",
			"en",
			"de",
			"es"
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
						// redirection of the type ex:type1
						new MultiTypeFilteringSail.TypedSail(
								// link each language sails
								SimpleLinkedSail.linkSails(
										Arrays.stream(languages).map(language ->
												new LuceneSailBuilder()
														.withDir(dir + "1-" + language)
														.withId(NAMESPACE + "lucene1_" + language)
														.withLanguageFiltering(language)
														.withEvaluationMode(TupleFunctionEvaluationMode.NATIVE)
														.build()
										),
										LuceneSail::setBaseSail,
										null
								),
								iri("type1")
						),
						// redirection of the type ex:type2
						new MultiTypeFilteringSail.TypedSail(
								// link each language sails
								SimpleLinkedSail.linkSails(
										Arrays.stream(languages).map(language ->
												new LuceneSailBuilder()
														.withDir(dir + "2-" + language)
														.withId(NAMESPACE + "lucene2_" + language)
														.withLanguageFiltering(language)
														.withEvaluationMode(TupleFunctionEvaluationMode.NATIVE)
														.build()
										),
										LuceneSail::setBaseSail,
										// the last LuceneSail mode should be TripleSource to optimize the queries
										sail -> sail.setEvaluationMode(TupleFunctionEvaluationMode.TRIPLE_SOURCE)
								),
								iri("type2")
						)
				),
				hybridStore,
				new PredicateSailFilter(iri("text"))
						.or(new PredicateSailFilter(iri("typeof")))
		);
	}

	@Test
	public void multiLanguageTypedIndexTest() {

	}
}
