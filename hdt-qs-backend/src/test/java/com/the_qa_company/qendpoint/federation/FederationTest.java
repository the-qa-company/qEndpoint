package com.the_qa_company.qendpoint.federation;

import com.google.common.collect.Sets;

import com.the_qa_company.qendpoint.functions.ParseDateFunction;
import com.the_qa_company.qendpoint.functions.Split;
import com.the_qa_company.qendpoint.store.EndpointStore;
import com.the_qa_company.qendpoint.utils.sail.SailTest;

import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.SKOS;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.explanation.Explanation;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Set;

public class FederationTest extends SailTest {

	@Override
	protected Sail configStore(EndpointStore endpoint) {
		return endpoint;
	}

	@Test
	public void wikibaseLabelService1() {
		add(VF.createStatement(VF.createIRI(SailTest.NAMESPACE + "s"), RDF.TYPE,
				VF.createIRI("http://the-qa-company.com/type")));
		add(VF.createStatement(VF.createIRI(SailTest.NAMESPACE + "s"), RDFS.LABEL,
				VF.createLiteral("My type1","en")));
		add(VF.createStatement(VF.createIRI(SailTest.NAMESPACE + "s"), RDFS.LABEL,
				VF.createLiteral("Mon type1","fr")));
		try (RepositoryConnection connection = repository.getConnection()) {
			String sparqlQuery = "SELECT ?s ?sLabel WHERE {\n" +
					"  ?s rdf:type <http://the-qa-company.com/type> .\n" +
					"  SERVICE <http://wikiba.se/ontology#label> { <http://www.bigdata.com/rdf#serviceParam> <http://wikiba.se/ontology#language> \"en\". }\n" +
					"}  limit 10";
			TupleQuery query = connection.prepareTupleQuery(SailTest.joinLines(SailTest.PREFIXES, sparqlQuery));
			try (TupleQueryResult result = query.evaluate()) {
				Assert.assertTrue(result.hasNext());
				while (result.hasNext()) {
					String element = result.next().getValue("sLabel").stringValue();
					Assert.assertEquals(element, "My type1");
				}
			}
		}
		try (RepositoryConnection connection = repository.getConnection()) {
			String sparqlQuery = "SELECT ?s ?sLabel WHERE {\n" +
					"  ?s rdf:type <http://the-qa-company.com/type> .\n" +
					"  SERVICE <http://wikiba.se/ontology#label> { <http://www.bigdata.com/rdf#serviceParam> <http://wikiba.se/ontology#language> \"fr,en\". }\n" +
					"}  limit 10";
			TupleQuery query = connection.prepareTupleQuery(SailTest.joinLines(SailTest.PREFIXES, sparqlQuery));
			try (TupleQueryResult result = query.evaluate()) {
				Assert.assertTrue(result.hasNext());
				while (result.hasNext()) {
					String element = result.next().getValue("sLabel").stringValue();
					Assert.assertEquals(element, "Mon type1");
				}
			}
		}
	}

	@Test
	// check alt label
	public void wikibaseLabelService2() {
		add(VF.createStatement(VF.createIRI(SailTest.NAMESPACE + "s"), RDF.TYPE,
				VF.createIRI("http://the-qa-company.com/type")));
		add(VF.createStatement(VF.createIRI(SailTest.NAMESPACE + "s"), SKOS.ALT_LABEL,
				VF.createLiteral("My alt type1","en")));
		try (RepositoryConnection connection = repository.getConnection()) {
			String sparqlQuery = "SELECT ?s ?sAltLabel WHERE {\n" +
					"  ?s rdf:type <http://the-qa-company.com/type> .\n" +
					"  SERVICE <http://wikiba.se/ontology#label> { <http://www.bigdata.com/rdf#serviceParam> <http://wikiba.se/ontology#language> \"en,fr\". }\n" +
					"}  limit 10";
			TupleQuery query = connection.prepareTupleQuery(SailTest.joinLines(SailTest.PREFIXES, sparqlQuery));
			try (TupleQueryResult result = query.evaluate()) {
				Assert.assertTrue(result.hasNext());
				while (result.hasNext()) {
					String element = result.next().getValue("sAltLabel").stringValue();
					Assert.assertEquals(element, "My alt type1");
				}
			}
		}
	}

	@Test
	// check description
	public void wikibaseLabelService3() {
		add(VF.createStatement(VF.createIRI(SailTest.NAMESPACE + "s"), RDF.TYPE,
				VF.createIRI("http://the-qa-company.com/type")));
		add(VF.createStatement(VF.createIRI(SailTest.NAMESPACE + "s"), VF.createIRI("https://schema.org/description"),
				VF.createLiteral("My alt type1","en")));
		try (RepositoryConnection connection = repository.getConnection()) {
			String sparqlQuery = "SELECT ?s ?sDescription WHERE {\n" +
					"  ?s rdf:type <http://the-qa-company.com/type> .\n" +
					"  SERVICE <http://wikiba.se/ontology#label> { <http://www.bigdata.com/rdf#serviceParam> <http://wikiba.se/ontology#language> \"en,fr\". }\n" +
					"}  limit 10";
			TupleQuery query = connection.prepareTupleQuery(SailTest.joinLines(SailTest.PREFIXES, sparqlQuery));
			try (TupleQueryResult result = query.evaluate()) {
				Assert.assertTrue(result.hasNext());
				while (result.hasNext()) {
					String element = result.next().getValue("sDescription").stringValue();
					Assert.assertEquals(element, "My alt type1");
				}
			}
		}
	}

}
