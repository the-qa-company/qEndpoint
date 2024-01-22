package com.the_qa_company.qendpoint;

import com.the_qa_company.qendpoint.compiler.CompiledSail;
import com.the_qa_company.qendpoint.compiler.SparqlRepository;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.store.EndpointFiles;
import com.the_qa_company.qendpoint.store.Utility;
import org.apache.commons.lang3.time.StopWatch;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.explanation.Explanation;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Stream;

public class TempTest {

	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();
	private SparqlRepository repository;


	@Before
	public void setupRepo() throws IOException {
		Path root = tempDir.newFolder().toPath();
		ClassLoader loader = getClass().getClassLoader();
		String filename = "2018_complete.nt";

		Path hdtstore = root.resolve("hdt-store");
		Path locationNative = root.resolve("native");

		Files.createDirectories(hdtstore);
		Files.createDirectories(locationNative);

		String indexName = "index.hdt";

		HDTOptions options = HDTOptions.of(
				// disable the default index (to use the custom indexes)
				HDTOptionsKeys.BITMAPTRIPLES_INDEX_NO_FOQ, true,
				// set the custom indexes we want
				HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS, "sop,ops,osp,pso,pos");


		try (HDT hdt = HDTManager.generateHDT(new Iterator<>() {
			@Override
			public boolean hasNext() {
				return false;
			}

			@Override
			public TripleString next() {
				return null;
			}
		}, Utility.EXAMPLE_NAMESPACE, options, null)) {
			hdt.saveToHDT(hdtstore.resolve(indexName).toAbsolutePath().toString(), null);
		} catch (Error | RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		repository = CompiledSail.compiler().withEndpointFiles(new EndpointFiles(locationNative, hdtstore, indexName))
				.compileToSparqlRepository();
		try (InputStream is = new BufferedInputStream(Objects.requireNonNull(loader.getResourceAsStream(filename),
				filename + " doesn't exist"))) {
			repository.loadFile(is, filename);
		}
	}

	@After
	public void after() {
		if (repository != null) {
			repository.shutDown();
		}
		repository = null;
	}

	@Test
	public void test() {
		try (SailRepositoryConnection connection = repository.getConnection()) {
			System.out.println();
			String query = """
					PREFIX epo: <http://data.europa.eu/a4g/ontology#>
					PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
					PREFIX legal: <https://www.w3.org/ns/legal#>
					PREFIX dcterms: <http://purl.org/dc/terms#>
					PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
					PREFIX dc: <http://purl.org/dc/elements/1.1/>
					      
					SELECT DISTINCT ?countryID ?year (COUNT(DISTINCT ?lot) AS ?amountLots) (SUM(if(?bidders = 1, 1, 0)) AS ?numSingleBidders) WHERE {
					      
					        ?proc a epo:Procedure .
					        ?proc epo:hasProcedureType ?p .
					        ?proc epo:hasProcurementScopeDividedIntoLot ?lot .
					      
					      	?stat epo:concernsSubmissionsForLot ?lot .
					      
					        ?stat a epo:SubmissionStatisticalInformation .
					        ?stat epo:hasReceivedTenders ?bidders .
					      	
					      	?resultnotice epo:refersToProcedure ?proc .
					        ?resultnotice epo:refersToRole ?buyerrole .					      	
					        ?resultnotice a epo:ResultNotice .
					        ?resultnotice epo:hasDispatchDate ?ddate .
					      
					      
					        {
					          SELECT DISTINCT ?buyerrole ?countryID WHERE {
					            ?org epo:hasBuyerType ?buytype .
					            FILTER (?buytype != <http://publications.europa.eu/resource/authority/buyer-legal-type/eu-int-org> )

					            ?buyerrole epo:playedBy ?org .
					            ?org legal:registeredAddress ?orgaddress .
					            ?orgaddress epo:hasCountryCode ?countrycode  .
					            ?countrycode dc:identifier ?countryID .
					      
					           }
					        }
					        
							FILTER ( ?p != <http://publications.europa.eu/resource/authority/procurement-procedure-type/neg-wo-call>)
												        BIND(year(xsd:dateTime(?ddate)) AS ?year) .

        
					} GROUP BY ?countryID ?year
					
							
					         """;

			System.out.println(query);

			Explanation explanation = runQuery(connection, query);
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println(explanation.toDot());
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println(explanation);
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();

		}

	}




	private static Explanation runQuery(SailRepositoryConnection connection, String query) {
		StopWatch stopWatch = StopWatch.createStarted();
		TupleQuery tupleQuery = connection.prepareTupleQuery(query);
		tupleQuery.setMaxExecutionTime(60*10);
		Explanation explain = tupleQuery.explain(Explanation.Level.Optimized);
//		System.out.println(explain);
//		System.out.println();
		System.out.println("Took: " + stopWatch.formatTime());

		return explain;

	}
}
