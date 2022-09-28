package com.the_qa_company.qendpoint.client;

import com.the_qa_company.qendpoint.Application;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.impl.SimpleLiteral;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EnableConfigurationProperties
public class RDF4JClientTest {

	@LocalServerPort
	private int port;

	@Test
	public void testAcceptHeaderGuessedRight() {
		SPARQLRepository repo = new SPARQLRepository("http://localhost:" + port + "/api/endpoint/sparql");

		try {
			String query = "INSERT DATA {<http://a> <http://a> \"Heating and cooling account for 50% of the EU’s total energy consumption, but at present only 19.1% of it is sourced from renewables, while in 5 countries out of 7 in North-West Europe the same ratio is below 8.2%. This makes heating & cooling an obvious target sector for efforts to increase the share of RES. D2Grids will do this by rolling out a proven but underutilised concept: 5th Generation District Heating and Cooling (5GDHC). 5GDHC is a highly optimised, demand-driven, self-regulating, energy management system for urban areas. Its key features are: 1) ultra-low temperature grid with decentralized energy plants; 2) closed thermal energy loops ensuring hot and cold exchange within and among buildings; 3) integration of thermal and electricity grids. Due to low grid temperature, low grid losses and efficient heat exchange mechanisms, total energy demand is substantially reduced, which can be effectively and securely supplied by RES, up to 100%. The objective of D2Grids is to increase the share of RES used for heating & cooling to 20% in NWE 10 years after the project ends, through accelerating the roll-out of 5GDCH systems. Uptake will be accelerated by (1) industrialisation of the system through developing a generic technology model and product standards; (2) boosting commercialization potential of 5GDHC systems through presenting solid business plans and attracting investors; (3) demonstrating the technology through impactful pilot investments in Bochum, Brunssum, Glasgow, Nottingham, and Paris-Saclay. Long-term effects will be ensured through (1) strategies, feasibility assessments and plans to sustain, scale up and roll out 5GDHC systems; (2) tailor-made training packages developed for industry, professionals and policy makers (3) transnational community building by setting up a 5GDHC Platform that ensures knowledge exchange and interaction among key target groups and (4) evaluations to draw recommendations on EU and national policies.\" }";
			try (RepositoryConnection connection = repo.getConnection()) {
				Update update = connection.prepareUpdate(query);
				update.execute();
			}

			query = "SELECT ?o WHERE {?s ?p ?o}";
			try (RepositoryConnection connection = repo.getConnection()) {
				try (TupleQueryResult resultSet = connection.prepareTupleQuery(query).evaluate()) {
					while (resultSet.hasNext()) {
						BindingSet b = resultSet.next();
						assertTrue(b.getBinding("o").getValue().isLiteral());
					}
				}
			}
		} finally {
			repo.shutDown();
		}
	}

}
