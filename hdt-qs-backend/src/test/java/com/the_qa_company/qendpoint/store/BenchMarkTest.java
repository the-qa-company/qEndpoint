package com.the_qa_company.qendpoint.store;

import org.apache.commons.lang3.time.StopWatch;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class BenchMarkTest {
	private static final Logger logger = LoggerFactory.getLogger(BenchMarkTest.class);
	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	@Test
	public void benchmarkDelete() throws IOException {
		StopWatch stopWatch = StopWatch.createStarted();
		File nativeStore = tempDir.newFolder("native-store");
		File hdtStore = tempDir.newFolder("hdt-store");
		HDTSpecification spec = new HDTSpecification();
		spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");
		HDT hdt = Utility.createTempHdtIndex(tempDir, false, true, spec);
		assert hdt != null;
		hdt.saveToHDT(hdtStore.getAbsolutePath() + "/" + EndpointStoreTest.HDT_INDEX_NAME, null);
		// printHDT(hdt);
		SailRepository endpointStore = new SailRepository(new EndpointStore(hdtStore.getAbsolutePath() + "/",
				EndpointStoreTest.HDT_INDEX_NAME, spec, nativeStore.getAbsolutePath() + "/", true));
		try (SailRepositoryConnection connection = endpointStore.getConnection()) {
			stopWatch.stop();
			int count = 100000;
			ValueFactory vf = connection.getValueFactory();
			stopWatch = StopWatch.createStarted();
			RepositoryResult<Statement> statements = connection.getStatements(null, null, null, true);
			while (statements.hasNext())
				statements.next();
			stopWatch.stop();
			logger.debug("Time to query all initially: {}", stopWatch.getTime(TimeUnit.MILLISECONDS));

			for (int i = 0; i < 10; i++) {
				stopWatch = StopWatch.createStarted();
				connection.begin();
				for (int j = count * i + 1; j <= count * (i + 1); j++) {
					connection.remove(Utility.getFakeStatement(vf, j));
				}
				connection.commit();
				logger.debug("# remaining triples: {}", (hdt.getTriples().getNumberOfElements() - count * (i + 1)));
				stopWatch.stop();
				logger.debug("Time to delete: {}", stopWatch.getTime(TimeUnit.MILLISECONDS));

				stopWatch = StopWatch.createStarted();
				statements = connection.getStatements(null, null, null, true);
				int c = 0;
				while (statements.hasNext()) {
					statements.next();
					c++;
				}
				stopWatch.stop();
				logger.debug("Time to query all: {}", stopWatch.getTime(TimeUnit.MILLISECONDS));
				logger.debug("Count: {}", c);
				assertEquals(connection.size(), hdt.getTriples().getNumberOfElements() - count * (i + 1));
				logger.debug("---------------------------------------");
			}
			logger.debug("Number of remaining triples: {}", connection.size());
			assertEquals(0, connection.size());
		}
	}

}
