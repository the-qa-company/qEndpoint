package com.the_qa_company.q_endpoint.utils.sail;

import com.the_qa_company.q_endpoint.hybridstore.HybridStore;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.evaluation.TupleFunctionEvaluationMode;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;
import org.junit.Test;

public class MultiTypeFilteringSailTest extends SailTest {
	@Override
	protected Sail configStore(HybridStore hybridStore) {
		// lucene sail to index the type 1 subject literals
		LuceneSail luceneSail1 = new LuceneSail();
		luceneSail1.setParameter(LuceneSail.LUCENE_DIR_KEY,
				hybridStore.getHybridStoreFiles().getLocationNative() + "lucene-index1");
		luceneSail1.setParameter(LuceneSail.INDEX_CLASS_KEY, LuceneSail.DEFAULT_INDEX_CLASS);
		luceneSail1.setParameter(LuceneSail.INDEX_ID, NAMESPACE + "lucene1");
		luceneSail1.setEvaluationMode(TupleFunctionEvaluationMode.NATIVE);

		// lucene sail to index the type 2 subject literals
		LuceneSail luceneSail2 = new LuceneSail();
		luceneSail2.setParameter(LuceneSail.LUCENE_DIR_KEY,
				hybridStore.getHybridStoreFiles().getLocationNative() + "lucene-index2");
		luceneSail2.setParameter(LuceneSail.INDEX_CLASS_KEY, LuceneSail.DEFAULT_INDEX_CLASS);
		luceneSail2.setParameter(LuceneSail.INDEX_ID, NAMESPACE + "lucene2");
		luceneSail2.setEvaluationMode(TupleFunctionEvaluationMode.TRIPLE_SOURCE);

		// config the multi type filtering sail
		return new MultiTypeFilteringSail(
				hybridStore,
				iri("typeof"),
				new MultiTypeFilteringSail.TypedSail(
						luceneSail1,
						iri("type1"),
						luceneSail1::setBaseSail
				),
				new MultiTypeFilteringSail.TypedSail(
						luceneSail2,
						iri("type2"),
						luceneSail2::setBaseSail
				)
		);
	}

	@Test
	public void noSplitIndexingTest() {
		Statement type1 = VF.createStatement(iri("a"), iri("typeof"), iri("type1"));
		Statement type2 = VF.createStatement(iri("b"), iri("typeof"), iri("type2"));
		Statement type3 = VF.createStatement(iri("c"), iri("typeof"), iri("type2"));
		Statement type4 = VF.createStatement(iri("d"), iri("typeof"), iri("type3"));

		Statement join = VF.createStatement(type1.getSubject(), iri("join"), type2.getSubject());

		Statement stmt1 = VF.createStatement(type1.getSubject(), iri("p"), VF.createLiteral("text a"));
		Statement stmt2 = VF.createStatement(type2.getSubject(), iri("p"), VF.createLiteral("text b"));
		Statement stmt3 = VF.createStatement(type3.getSubject(), iri("p"), VF.createLiteral("text c"));
		Statement stmt4 = VF.createStatement(type4.getSubject(), iri("p"), VF.createLiteral("text d"));

		add(
				type1,
				type2,
				type3,
				type4,
				join,
				stmt1,
				stmt2,
				stmt3,
				stmt4
		);

		// test query lucene 1
		assertSelect(
				new LuceneSelectWhereBuilder("r", "text")
						.withIndexId("ex:lucene1")
						.buildWithSelectWhereClause(),
				new SelectResultRow().withValue("r", stmt1.getSubject())
		);
		// test query lucene 2
		assertSelect(
				new LuceneSelectWhereBuilder("r", "text")
						.withIndexId("ex:lucene2")
						.buildWithSelectWhereClause(),
				new SelectResultRow().withValue("r", stmt2.getSubject()),
				new SelectResultRow().withValue("r", stmt3.getSubject())
		);
		// test query lucene 1 and lucene 2 with UNION
		assertSelect(
				joinLines(
						"SELECT * {",
						" {",
						new LuceneSelectWhereBuilder("r1", "text")
								.withIndexId("ex:lucene1")
								.build(),
						" } UNION {",
						new LuceneSelectWhereBuilder("r2", "text")
								.withIndexId("ex:lucene2")
								.build(),
						" }",
						"}"
				),
				new SelectResultRow().withValue("r1", stmt1.getSubject()),
				new SelectResultRow().withValue("r2", stmt2.getSubject()),
				new SelectResultRow().withValue("r2", stmt3.getSubject())
		);

		// test query lucene 1 and lucene 2 with JOIN
		assertSelect(
				joinLines(
						"SELECT * {",
						new LuceneSelectWhereBuilder("r1", "text")
								.withIndexId("ex:lucene1")
								.build(),
						new LuceneSelectWhereBuilder("r2", "text")
								.withIndexId("ex:lucene2")
								.build(),
						"  ?r1 ex:join ?r2 .",
						"}"
				),
				new SelectResultRow()
						.withValue("r1", stmt1.getSubject())
						.withValue("r2", stmt2.getSubject())
		);
	}
	@Test
	public void splitIndexingTest() {
		Statement type1 = VF.createStatement(iri("a"), iri("typeof"), iri("type1"));
		Statement type2 = VF.createStatement(iri("b"), iri("typeof"), iri("type2"));
		Statement type3 = VF.createStatement(iri("c"), iri("typeof"), iri("type2"));
		Statement type4 = VF.createStatement(iri("d"), iri("typeof"), iri("type3"));

		Statement join = VF.createStatement(type1.getSubject(), iri("join"), type2.getSubject());

		Statement stmt1 = VF.createStatement(type1.getSubject(), iri("p"), VF.createLiteral("text a"));
		Statement stmt2 = VF.createStatement(type2.getSubject(), iri("p"), VF.createLiteral("text b"));
		Statement stmt3 = VF.createStatement(type3.getSubject(), iri("p"), VF.createLiteral("text c"));
		Statement stmt4 = VF.createStatement(type4.getSubject(), iri("p"), VF.createLiteral("text d"));

		add(
				type1,
				type2,
				type3,
				type4);
		add(join);
		add(
				stmt1,
				stmt2,
				stmt3,
				stmt4
		);

		// test query lucene 1
		assertSelect(
				new LuceneSelectWhereBuilder("r", "text")
						.withIndexId("ex:lucene1")
						.buildWithSelectWhereClause(),
				new SelectResultRow().withValue("r", stmt1.getSubject())
		);
		// test query lucene 2
		assertSelect(
				new LuceneSelectWhereBuilder("r", "text")
						.withIndexId("ex:lucene2")
						.buildWithSelectWhereClause(),
				new SelectResultRow().withValue("r", stmt2.getSubject()),
				new SelectResultRow().withValue("r", stmt3.getSubject())
		);
		// test query lucene 1 and lucene 2 with UNION
		assertSelect(
				joinLines(
						"SELECT * {",
						" {",
						new LuceneSelectWhereBuilder("r1", "text")
								.withIndexId("ex:lucene1")
								.build(),
						" } UNION {",
						new LuceneSelectWhereBuilder("r2", "text")
								.withIndexId("ex:lucene2")
								.build(),
						" }",
						"}"
				),
				new SelectResultRow().withValue("r1", stmt1.getSubject()),
				new SelectResultRow().withValue("r2", stmt2.getSubject()),
				new SelectResultRow().withValue("r2", stmt3.getSubject())
		);

		// test query lucene 1 and lucene 2 with JOIN
		assertSelect(
				joinLines(
						"SELECT * {",
						new LuceneSelectWhereBuilder("r1", "text")
								.withIndexId("ex:lucene1")
								.build(),
						new LuceneSelectWhereBuilder("r2", "text")
								.withIndexId("ex:lucene2")
								.build(),
						"  ?r1 ex:join ?r2 .",
						"}"
				),
				new SelectResultRow()
						.withValue("r1", stmt1.getSubject())
						.withValue("r2", stmt2.getSubject())
		);
	}
}
