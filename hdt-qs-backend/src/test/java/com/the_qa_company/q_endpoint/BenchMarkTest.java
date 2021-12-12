package com.the_qa_company.q_endpoint;

import com.the_qa_company.q_endpoint.hybridstore.HybridStore;

import org.apache.commons.lang3.time.StopWatch;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.options.HDTSpecification;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class BenchMarkTest {
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Test
    public void benchmarkDelete() {
        try {
            StopWatch stopWatch = StopWatch.createStarted();
            File nativeStore = tempDir.newFolder("native-store");
            File hdtStore = tempDir.newFolder("hdt-store");
            HDT hdt = Utility.createTempHdtIndex(tempDir, false, true);
            assert hdt != null;
            hdt.saveToHDT(hdtStore.getAbsolutePath() + "/index.hdt", null);
            //printHDT(hdt);
            HDTSpecification spec = new HDTSpecification();
            spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");
            SailRepository hybridStore = new SailRepository(
                    new HybridStore(
                            hdtStore.getAbsolutePath() + "/", spec, nativeStore.getAbsolutePath() + "/", true
                    ));
            try (SailRepositoryConnection connection = hybridStore.getConnection()) {
                stopWatch.stop();
                int count = 100000;
                ValueFactory vf = connection.getValueFactory();
                String ex = "http://example.com/";
                stopWatch = StopWatch.createStarted();
                RepositoryResult<Statement> statements = connection.getStatements(null, null, null, true);
                while (statements.hasNext())
                    statements.next();
                stopWatch.stop();
                System.out.println("Time to query all initialiy: " + stopWatch.getTime(TimeUnit.MILLISECONDS));

                for (int i = 0; i < 10; i++) {
                    stopWatch = StopWatch.createStarted();
                    connection.begin();
                    for (int j = count * i + 1; j <= count * (i + 1); j++) {
                        IRI entity = vf.createIRI(ex, "person" + j);
                        Statement stm = vf.createStatement(entity, RDF.TYPE, FOAF.PERSON);
                        connection.remove(stm);
                    }
                    connection.commit();
                    assert hdt != null;
                    System.out.println("# remaining triples:" + (hdt.getTriples().getNumberOfElements() - count * (i + 1)));
                    stopWatch.stop();
                    System.out.println("Time to delete: " + stopWatch.getTime(TimeUnit.MILLISECONDS));

                    stopWatch = StopWatch.createStarted();
                    statements = connection.getStatements(null, null, null, true);
                    int c = 0;
                    while (statements.hasNext()) {
                        statements.next();
                        c++;
                    }
                    stopWatch.stop();
                    System.out.println("Time to query all: " + stopWatch.getTime(TimeUnit.MILLISECONDS));
                    System.out.println("Count:" + c);
                    assertEquals(connection.size(), hdt.getTriples().getNumberOfElements() - count * (i + 1));
                    System.out.println("---------------------------------------");
                }
                System.out.println("Number of remaining triples: " + connection.size());
                assertEquals(0, connection.size());
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail("Catched Exception");
        }
    }

}
