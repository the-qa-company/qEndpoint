package com.the_qa_company.q_endpoint.utils.sail.filter;

import com.the_qa_company.q_endpoint.hybridstore.HybridStore;
import com.the_qa_company.q_endpoint.utils.sail.FilteringSail;
import com.the_qa_company.q_endpoint.utils.sail.SailTest;
import com.the_qa_company.q_endpoint.utils.sail.helpers.LuceneSailBuilder;
import com.the_qa_company.q_endpoint.utils.sail.linked.LinkedSail;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.evaluation.TupleFunctionEvaluationMode;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;
import org.junit.Test;

import java.util.function.Function;

public class TypeFilterTest extends SailTest {
	private Function<SailConnection, SailFilter> filterBuilder;

	@Override
	protected Sail configStore(HybridStore hybridStore) {
		LinkedSail<LuceneSail> luceneSail = new LuceneSailBuilder()
				.withDir(hybridStore.getHybridStoreFiles().getLocationNative() + "lucene-index")
				.withId(NAMESPACE + "lucene")
				.withEvaluationMode(TupleFunctionEvaluationMode.NATIVE)
				.buildLinked();
		filterBuilder = (connection -> new TypeSailFilter(connection, iri("oftype"), iri("mytype1")));
		// basic implementation
		return new FilteringSail(
				luceneSail,
				hybridStore,
				// use the apply and not filterBuilder to allow update of the builder
				connection -> filterBuilder.apply(connection)
		);
	}

	@Test
	public void noSplitAddTypeTest() {
		Statement stmttype1 = VF.createStatement(iri("a"), iri("oftype"), iri("mytype1"));
		Statement stmttype2 = VF.createStatement(iri("b"), iri("oftype"), iri("mytype2"));
		Statement stmttype3 = VF.createStatement(iri("c"), iri("oftype"), iri("mytype3"));
		Statement stmt1 = VF.createStatement(stmttype1.getSubject(), iri("p"), VF.createLiteral("text a"));
		Statement stmt2 = VF.createStatement(stmttype2.getSubject(), iri("p"), VF.createLiteral("text b"));
		Statement stmt3 = VF.createStatement(stmttype3.getSubject(), iri("p"), VF.createLiteral("text c"));

		add(
				stmttype1,
				stmttype2,
				stmt1,
				stmt2
		);

		assertSelect(
				new LuceneSelectWhereBuilder("r", "text")
						.withIndexId("ex:lucene")
						.buildWithSelectWhereClause(),
				new SelectResultRow().withValue("r", stmt1.getSubject())
		);

		final Function<SailConnection, SailFilter> oldFilterBuilder = filterBuilder;
		filterBuilder =
				(connection ->
						oldFilterBuilder.apply(connection)
								.or(new TypeSailFilter(connection, iri("oftype"), iri("mytype3")))
				);

		add(
				stmttype3,
				stmt3
		);

		assertSelect(
				new LuceneSelectWhereBuilder("r", "text")
						.withIndexId("ex:lucene")
						.buildWithSelectWhereClause(),
				new SelectResultRow().withValue("r", stmt1.getSubject()),
				new SelectResultRow().withValue("r", stmt3.getSubject())
		);
	}

	@Test
	public void splitAddTypeTest() {
		Statement stmttype1 = VF.createStatement(iri("a"), iri("oftype"), iri("mytype1"));
		Statement stmttype2 = VF.createStatement(iri("b"), iri("oftype"), iri("mytype2"));
		Statement stmttype3 = VF.createStatement(iri("c"), iri("oftype"), iri("mytype3"));
		Statement stmt1 = VF.createStatement(stmttype1.getSubject(), iri("p"), VF.createLiteral("text a"));
		Statement stmt2 = VF.createStatement(stmttype2.getSubject(), iri("p"), VF.createLiteral("text b"));
		Statement stmt3 = VF.createStatement(stmttype3.getSubject(), iri("p"), VF.createLiteral("text c"));

		add(
				stmttype1,
				stmttype2
		);
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

		final Function<SailConnection, SailFilter> oldFilterBuilder = filterBuilder;
		filterBuilder =
				(connection ->
						oldFilterBuilder
								.apply(connection) // previous type filter
								.or(new TypeSailFilter(connection, iri("oftype"), iri("mytype3")))
				);

		add(stmttype3);
		add(stmt3);

		assertSelect(
				new LuceneSelectWhereBuilder("r", "text")
						.withIndexId("ex:lucene")
						.buildWithSelectWhereClause(),
				new SelectResultRow().withValue("r", stmt1.getSubject()),
				new SelectResultRow().withValue("r", stmt3.getSubject())
		);

	}

}
