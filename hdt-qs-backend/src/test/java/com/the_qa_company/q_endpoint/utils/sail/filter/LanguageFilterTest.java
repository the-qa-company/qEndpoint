package com.the_qa_company.q_endpoint.utils.sail.filter;

import com.the_qa_company.q_endpoint.hybridstore.HybridStore;
import com.the_qa_company.q_endpoint.utils.sail.FilteringSail;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.evaluation.TupleFunctionEvaluationMode;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;
import org.junit.Test;

public class LanguageFilterTest extends SailTest {
	/**
	 * the filter to test
	 */
	private LanguageSailFilter filter;
	@Override
	protected Sail configStore(HybridStore hybridStore) {
		LuceneSail luceneSail = new LuceneSail();
		luceneSail.setParameter(LuceneSail.LUCENE_DIR_KEY,
				hybridStore.getHybridStoreFiles().getLocationNative() + "lucene-index");
		luceneSail.setParameter(LuceneSail.INDEX_CLASS_KEY, LuceneSail.DEFAULT_INDEX_CLASS);
		luceneSail.setParameter(LuceneSail.INDEX_ID, NAMESPACE + "fr_lucene");
		luceneSail.setEvaluationMode(TupleFunctionEvaluationMode.TRIPLE_SOURCE);
		// basic implementation
		filter = new LanguageSailFilter("fr", false, true);
		return new FilteringSail(luceneSail, hybridStore, luceneSail::setBaseSail, filter);
	}

	@Test
	public void languageInjectionTest() {
		Statement lit1 = VF.createStatement(iri("a"), iri("p"), VF.createLiteral("text a", "fr"));
		Statement lit2 = VF.createStatement(iri("b"), iri("p"), VF.createLiteral("text b", "en"));
		Statement lit3 = VF.createStatement(iri("c"), iri("p"), VF.createLiteral("text c"));
		add(
				lit1,
				lit2,
				lit3
		);

		assertSelect(
				SPO_QUERY,
				new SelectResultRow().withSPO(lit1),
				new SelectResultRow().withSPO(lit2),
				new SelectResultRow().withSPO(lit3)
		);

		assertSelect(
				joinLines(
						"SELECT * {",
						new LuceneSelectWhereBuilder("subj", "text").withIndexId("ex:fr_lucene").build(),
						"}"
				),
				new SelectResultRow().withValue("subj", lit1.getSubject())
		);
	}
	@Test
	public void languageNoLangInjectionTest() {
		filter.setAcceptNoLanguageLiterals(true);
		Statement lit1 = VF.createStatement(iri("a"), iri("p"), VF.createLiteral("text a", "fr"));
		Statement lit2 = VF.createStatement(iri("b"), iri("p"), VF.createLiteral("text b", "en"));
		Statement lit3 = VF.createStatement(iri("c"), iri("p"), VF.createLiteral("text c"));
		add(
				lit1,
				lit2,
				lit3
		);

		assertSelect(
				SPO_QUERY,
				new SelectResultRow().withSPO(lit1),
				new SelectResultRow().withSPO(lit2),
				new SelectResultRow().withSPO(lit3)
		);

		assertSelect(
				joinLines(
						"SELECT * {",
						new LuceneSelectWhereBuilder("subj", "text").withIndexId("ex:fr_lucene").build(),
						"}"
				),
				new SelectResultRow().withValue("subj", lit1.getSubject()),
				new SelectResultRow().withValue("subj", lit3.getSubject())
		);
	}

	@Test
	public void languageNoExprInjectionTest() {
		filter.setShouldHandleExpression(false);
		Statement lit1 = VF.createStatement(iri("a"), iri("p"), VF.createLiteral("text a", "fr"));
		Statement lit2 = VF.createStatement(iri("b"), iri("p"), VF.createLiteral("text b", "en"));
		Statement lit3 = VF.createStatement(iri("c"), iri("p"), VF.createLiteral("text c"));
		add(
				lit1,
				lit2,
				lit3
		);

		assertSelect(
				SPO_QUERY,
				new SelectResultRow().withSPO(lit1),
				new SelectResultRow().withSPO(lit2),
				new SelectResultRow().withSPO(lit3)
		);

		// no expression allowed, it shouldn't pass by the lucene sail
		assertSelect(
				joinLines(
						"SELECT * {",
						new LuceneSelectWhereBuilder("subj", "text").withIndexId("ex:fr_lucene").build(),
						"}"
				)
		);

		// enable expression, now we can  pass by the lucene sail to query it
		filter.setShouldHandleExpression(true);
		assertSelect(
				joinLines(
						"SELECT * {",
						new LuceneSelectWhereBuilder("subj", "text").withIndexId("ex:fr_lucene").build(),
						"}"
				),
				new SelectResultRow().withValue("subj", lit1.getSubject())
		);
	}
}
