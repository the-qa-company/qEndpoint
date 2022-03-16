package com.the_qa_company.q_endpoint.utils.sail.filter;

import com.the_qa_company.q_endpoint.hybridstore.HybridStore;
import com.the_qa_company.q_endpoint.utils.sail.FilteringSail;
import com.the_qa_company.q_endpoint.utils.sail.linked.LinkedSail;
import com.the_qa_company.q_endpoint.utils.sail.linked.SimpleLinkedSail;
import com.the_qa_company.q_endpoint.utils.sail.helpers.LuceneSailBuilder;
import com.the_qa_company.q_endpoint.utils.sail.SailTest;
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
		LinkedSail<LuceneSail> luceneSail = new LuceneSailBuilder()
				.withDir(hybridStore.getHybridStoreFiles().getLocationNative() + "lucene-index")
				.withId(NAMESPACE + "fr_lucene")
				.withEvaluationMode(TupleFunctionEvaluationMode.NATIVE)
				.buildLinked();
		// basic implementation
		filter = new LanguageSailFilter("fr", false, true);
		return new FilteringSail(luceneSail, hybridStore, filter);
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
				new LuceneSelectWhereBuilder("subj", "text")
						.withIndexId("ex:fr_lucene").
						buildWithSelectWhereClause(),
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
				new LuceneSelectWhereBuilder("subj", "text")
						.withIndexId("ex:fr_lucene")
						.buildWithSelectWhereClause(),
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
				new LuceneSelectWhereBuilder("subj", "text")
						.withIndexId("ex:fr_lucene")
						.buildWithSelectWhereClause()
		);

		// enable expression, now we can  pass by the lucene sail to query it
		filter.setShouldHandleExpression(true);
		assertSelect(
				new LuceneSelectWhereBuilder("subj", "text")
						.withIndexId("ex:fr_lucene")
						.buildWithSelectWhereClause(),
				new SelectResultRow().withValue("subj", lit1.getSubject())
		);

		remove(
				lit1
		);
		assertSelect(
				new LuceneSelectWhereBuilder("subj", "text")
						.withIndexId("ex:fr_lucene")
						.buildWithSelectWhereClause()
		);
	}
}
