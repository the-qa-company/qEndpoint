package com.the_qa_company.q_endpoint;

import com.the_qa_company.q_endpoint.hybridstore.HybridStore;
import com.the_qa_company.q_endpoint.hybridstore.MergeRunnable;
import com.the_qa_company.q_endpoint.hybridstore.MergeRunnableStopPoint;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.options.HDTSpecification;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class MergeRestartTest {
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();
    HDTSpecification spec;

    @Before
    public void setUp() {
        spec = new HDTSpecification();
        spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");
    }

    public void mergeRestartTest(MergeRunnableStopPoint stopPoint) throws IOException, InterruptedException {
        // @todo: create basic store
        MergeRunnable.setDebugMergeThread(true);
        File nativeStore = tempDir.newFolder("native-store");
        File hdtStore = tempDir.newFolder("hdt-store");
        HDT hdt = com.the_qa_company.q_endpoint.Utility.createTempHdtIndex(tempDir, false, false, spec);
        assert hdt != null;
        hdt.saveToHDT(hdtStore.getAbsolutePath() + "/index.hdt", null);
//            printHDT(hdt);
        MergeRunnable.setStopPoint(stopPoint);
        HybridStore store = new HybridStore(
                hdtStore.getAbsolutePath() + "/", spec, nativeStore.getAbsolutePath() + "/", false
        );
        System.out.println("launching merge with stopPoint=" + stopPoint.name().toLowerCase());

        store.setThreshold(2);
        SailRepository hybridStore = new SailRepository(store);

        try (RepositoryConnection connection = hybridStore.getConnection()) {
            ValueFactory vf = connection.getValueFactory();
            String ex = "http://example.com/";
            IRI ali = vf.createIRI(ex, "Ali");
            connection.add(ali, RDF.TYPE, FOAF.PERSON);
            IRI dennis = vf.createIRI(ex, "Dennis");
            connection.add(dennis, RDF.TYPE, FOAF.PERSON);

            // with given THRESHOLD = 2, the hdt index will be merged with all triples from current native store
            IRI pierre = vf.createIRI(ex, "Pierre");
            connection.add(pierre, RDF.TYPE, FOAF.PERSON);

            IRI guo = vf.createIRI(ex, "Guo");
            connection.remove(guo, RDF.TYPE, FOAF.PERSON);
            // wait for merge to be done because it's on a separate thread

            RepositoryResult<Statement> sts = connection.getStatements(null, null, null, true);
            int count = 0;
            while (sts.hasNext()) {
                System.out.println(sts.next());
                count++;
            }
            // 1 triple hdt, 2 triples native a, 1 triple native b -1 triple removed from hdt
            assertEquals(3, count);
            Thread.sleep(3000);


            RepositoryResult<Statement> sts2 = connection.getStatements(null, null, null, true);
            count = 0;
            while (sts2.hasNext()) {
                System.out.println(sts2.next());
                count++;
            }
            // 2 triples hdt, 0 triples native a, 1 triple native b
            assertEquals(3, count);
            Files.deleteIfExists(Paths.get("index.hdt"));
            Files.deleteIfExists(Paths.get("index.hdt.index.v1-1"));
            Files.deleteIfExists(Paths.get("index.nt"));

        }

        MergeRunnable.debugWaitMerge();
        store.shutDown();
        // recreate a basic store
        HDT hdt2 = com.the_qa_company.q_endpoint.Utility.createTempHdtIndex(tempDir, false, false, spec);
        assert hdt2 != null;
        hdt2.saveToHDT(hdtStore.getAbsolutePath() + "/index.hdt", null);
//            printHDT(hdt);
        HybridStore store2 = new HybridStore(
                hdtStore.getAbsolutePath() + "/", spec, nativeStore.getAbsolutePath() + "/", false
        );
        SailRepository hybridStore2 = new SailRepository(store2);
        // wait for the complete merge
        MergeRunnable.debugWaitMerge();

        // @todo: check if no issue with the new store

        try (RepositoryConnection connection = hybridStore2.getConnection()) {
            RepositoryResult<Statement> sts2 = connection.getStatements(null, null, null, true);
            int count = 0;
            while (sts2.hasNext()) {
                System.out.println(sts2.next());
                count++;
            }
            assertEquals(3, count);
        }


        MergeRunnable.setDebugMergeThread(false);
    }

    @Test
    public void mergeRestartStep1StartTest() throws IOException, InterruptedException {
        mergeRestartTest(MergeRunnableStopPoint.STEP1_START);
    }
    @Test
    public void mergeRestartStep1EndTest() throws IOException, InterruptedException {
        mergeRestartTest(MergeRunnableStopPoint.STEP1_END);
    }
    @Test
    public void mergeRestartStep2StartTest() throws IOException, InterruptedException {
        mergeRestartTest(MergeRunnableStopPoint.STEP2_START);
    }
    @Test
    public void mergeRestartStep2EndTest() throws IOException, InterruptedException {
        mergeRestartTest(MergeRunnableStopPoint.STEP2_END);
    }
    @Test
    public void mergeRestartStep3StartTest() throws IOException, InterruptedException {
        mergeRestartTest(MergeRunnableStopPoint.STEP3_START);
    }
    @Test
    public void mergeRestartStep3EndTest() throws IOException, InterruptedException {
        mergeRestartTest(MergeRunnableStopPoint.STEP3_END);
    }
    @Test
    public void mergeRestartMergeEndTest() throws IOException, InterruptedException {
        mergeRestartTest(MergeRunnableStopPoint.MERGE_END);
    }

    @Test
    @Ignore("not yet implemented")
    public void mergeRestartAllPointsTest() throws IOException, InterruptedException {
        for (MergeRunnableStopPoint stopPoint : MergeRunnableStopPoint.values()) {
            mergeRestartTest(stopPoint);
        }
    }
}
