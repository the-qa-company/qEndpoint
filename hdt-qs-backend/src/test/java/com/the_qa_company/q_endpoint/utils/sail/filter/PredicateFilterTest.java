package com.the_qa_company.q_endpoint.utils.sail.filter;

import com.the_qa_company.q_endpoint.hybridstore.HybridStore;
import com.the_qa_company.q_endpoint.utils.sail.FilteringSail;
import com.the_qa_company.q_endpoint.utils.sail.SailTest;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.evaluation.TupleFunctionEvaluationMode;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;
import org.junit.Test;

public class PredicateFilterTest extends SailTest {
	/**
	 * the filter to test
	 */
	private SailFilter filter;

	@Override
	protected Sail configStore(HybridStore hybridStore) {
		LuceneSail luceneSail = new LuceneSail();
		luceneSail.setParameter(LuceneSail.LUCENE_DIR_KEY,
				hybridStore.getHybridStoreFiles().getLocationNative() + "lucene-index");
		luceneSail.setParameter(LuceneSail.INDEX_CLASS_KEY, LuceneSail.DEFAULT_INDEX_CLASS);
		luceneSail.setParameter(LuceneSail.INDEX_ID, NAMESPACE + "lucene");
		luceneSail.setEvaluationMode(TupleFunctionEvaluationMode.TRIPLE_SOURCE);
		// basic implementation
		filter = new PredicateSailFilter(iri("p"));
		return new FilteringSail(luceneSail, hybridStore, luceneSail::setBaseSail, connection -> filter);
	}

	@Test
	public void multipleTextTest() {
		Statement stmt1 = VF.createStatement(iri("a"), iri("p"), VF.createLiteral("text a"));
		Statement stmt2 = VF.createStatement(iri("b"), iri("p2"), VF.createLiteral("text b"));
		Statement stmt3 = VF.createStatement(iri("c"), iri("p2"), VF.createLiteral("text c"));
		Statement stmt4 = VF.createStatement(iri("d"), iri("p3"), VF.createLiteral("text d"));

		add(
				stmt1,
				stmt2
		);

		assertSelect(
				new LuceneSelectWhereBuilder("r", "text")
						.withIndexId("ex:lucene")
						.buildWithSelectWhereClause(),
				new SelectResultRow().withValue("r", stmt1.getSubject())
		);

		// add a filter for the ex:p3 predicate
		filter = filter.or(new PredicateSailFilter(iri("p3")));

		// nothing should be updated here
		assertSelect(
				new LuceneSelectWhereBuilder("r", "text")
						.withIndexId("ex:lucene")
						.buildWithSelectWhereClause(),
				new SelectResultRow().withValue("r", stmt1.getSubject())
		);

		add(
				stmt3,
				stmt4
		);

		assertSelect(
				new LuceneSelectWhereBuilder("r", "text")
						.withIndexId("ex:lucene")
						.buildWithSelectWhereClause(),
				new SelectResultRow().withValue("r", stmt1.getSubject()),
				new SelectResultRow().withValue("r", stmt4.getSubject())
		);

		// we check that we can get by property element
		assertSelect(
				new LuceneSelectWhereBuilder("r", "text")
						.withIndexId("ex:lucene")
						.withProperty("ex:p")
						.buildWithSelectWhereClause(),
				new SelectResultRow().withValue("r", stmt1.getSubject())
		);
		assertSelect(
				new LuceneSelectWhereBuilder("r", "text")
						.withIndexId("ex:lucene")
						.withProperty("ex:p3")
						.buildWithSelectWhereClause(),
				new SelectResultRow().withValue("r", stmt4.getSubject())
		);
		// no indexing for ex:p2
		assertSelect(
				new LuceneSelectWhereBuilder("r", "text")
						.withIndexId("ex:lucene")
						.withProperty("ex:p2")
						.buildWithSelectWhereClause()
		);

		remove(
				stmt4
		);
		assertSelect(
				new LuceneSelectWhereBuilder("r", "text")
						.withIndexId("ex:lucene")
						.buildWithSelectWhereClause(),
				new SelectResultRow().withValue("r", stmt1.getSubject())
		);

	}

}
