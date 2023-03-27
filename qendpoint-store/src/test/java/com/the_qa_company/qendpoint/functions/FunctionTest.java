package com.the_qa_company.qendpoint.functions;

import com.google.common.collect.Sets;
import com.the_qa_company.qendpoint.store.EndpointStore;
import com.the_qa_company.qendpoint.utils.sail.SailTest;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Set;

public class FunctionTest extends SailTest {

	@Override
	protected Sail configStore(EndpointStore endpoint) {
		return endpoint;
	}

	@Test
	@Ignore("TODO: find how to use tuple functions") // TODO: find how to use
														// tuple functions
	public void splitFunctionTest() {
		add(VF.createStatement(VF.createIRI(SailTest.NAMESPACE + "s"), VF.createIRI(SailTest.NAMESPACE + "p"),
				VF.createLiteral("I love my plant2")));
		Set<String> set = Sets.newHashSet("I love my plant2".split(" "));
		try (RepositoryConnection connection = repository.getConnection()) {
			TupleQuery query = connection.prepareTupleQuery(SailTest.joinLines(SailTest.PREFIXES, "SELECT ?element {",
					" ?s ex:p ?o .", " ?element <" + Split.URI + "> (?o, ' ') .", "}"));
			try (TupleQueryResult result = query.evaluate()) {
				while (result.hasNext()) {
					String element = result.next().getValue("element").stringValue();
					Assert.assertTrue("element not excepted: " + element, set.remove(element));
				}
			}
			Assert.assertTrue(set.isEmpty());
		}
	}

	@Test
	public void dateFunctionTest() {
		add(VF.createStatement(VF.createIRI(SailTest.NAMESPACE + "s"), VF.createIRI(SailTest.NAMESPACE + "p"),
				VF.createLiteral("1792-09-22", XSD.DATE)));
		try (RepositoryConnection connection = repository.getConnection()) {
			TupleQuery query = connection.prepareTupleQuery(SailTest.joinLines(SailTest.PREFIXES, "SELECT ?date {",
					" ex:s ex:p ?o .", " BIND (<" + ParseDateFunction.URI + ">(?o, 'yyyy-MM') AS ?date)", "}"));
			try (TupleQueryResult result = query.evaluate()) {
				Assert.assertTrue(result.hasNext());
				String element = result.next().getValue("date").stringValue();
				Assert.assertEquals("1792-09", element);
				Assert.assertFalse(result.hasNext());
			}
		}
	}

}
