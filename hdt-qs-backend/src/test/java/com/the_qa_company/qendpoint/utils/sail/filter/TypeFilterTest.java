package com.the_qa_company.qendpoint.utils.sail.filter;

import com.the_qa_company.qendpoint.store.EndpointStore;
import com.the_qa_company.qendpoint.utils.sail.FilteringSail;
import com.the_qa_company.qendpoint.utils.sail.SailTest;
import com.the_qa_company.qendpoint.utils.sail.helpers.LuceneSailBuilder;
import com.the_qa_company.qendpoint.utils.sail.linked.LinkedSail;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.evaluation.TupleFunctionEvaluationMode;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;
import org.junit.Test;

import java.util.function.BiFunction;

public class TypeFilterTest extends SailTest {
	private BiFunction<FilteringSail, SailConnection, SailFilter> filterBuilder;

	@Override
	protected Sail configStore(EndpointStore endpoint) {
		LinkedSail<LuceneSail> luceneSail = new LuceneSailBuilder()
				.withDir(endpoint.getEndpointFiles().getLocationNative() + "lucene-index")
				.withId(NAMESPACE + "lucene")
				.withEvaluationMode(TupleFunctionEvaluationMode.NATIVE)
				.buildLinked();
		filterBuilder = ((sail, connection) -> new TypeSailFilter(sail, iri("oftype"), iri("mytype1")));
		// basic implementation
		return new FilteringSail(
				luceneSail,
				endpoint,
				// use the apply and not filterBuilder to allow update of the builder
				(sail, connection) -> filterBuilder.apply(sail, connection)
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

		final BiFunction<FilteringSail, SailConnection, SailFilter> oldFilterBuilder = filterBuilder;
		filterBuilder =
				((sail, connection) ->
						oldFilterBuilder.apply(sail, connection)
								.or(new TypeSailFilter(sail, iri("oftype"), iri("mytype3")))
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

		final BiFunction<FilteringSail, SailConnection, SailFilter> oldFilterBuilder = filterBuilder;
		filterBuilder =
				((sail, connection) ->
						oldFilterBuilder
								.apply(sail, connection) // previous type filter
								.or(new TypeSailFilter(sail, iri("oftype"), iri("mytype3")))
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
