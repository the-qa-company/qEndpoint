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
																.withId(NAMESPACE + "lucene1_" + language)
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

	@Test
	public void multiLanguageTypedIndexTest() {
		add(
				VF.createStatement(iri("a"), iri("typeof"), iri("type1")),
				VF.createStatement(iri("a"), iri("prop"), iri("b"))
		);
		for (String lang : languages) {
			add(VF.createStatement(iri("a"), iri("text"), VF.createLiteral("text a", lang)));
		}

		add(
				VF.createStatement(iri("b"), iri("typeof"), iri("type1"))
		);

		for (String lang : languages) {
			add(VF.createStatement(iri("b"), iri("text"), VF.createLiteral("text b", lang)));
		}

		add(

				VF.createStatement(iri("c"), iri("typeof"), iri("type2"))
		);

		for (String lang : languages) {
			add(VF.createStatement(iri("c"), iri("text"), VF.createLiteral("text c", lang)));
		}

		for (String lang : languages) {
		}
	}
}
