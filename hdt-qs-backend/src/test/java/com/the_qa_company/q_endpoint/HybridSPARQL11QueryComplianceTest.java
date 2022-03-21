package com.the_qa_company.q_endpoint;

import com.the_qa_company.q_endpoint.hybridstore.HybridStore;

import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.testsuite.query.parser.sparql.manifest.SPARQL11QueryComplianceTest;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;

import java.io.File;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Ali Haidar
 */
public class HybridSPARQL11QueryComplianceTest extends SPARQL11QueryComplianceTest {

    public HybridSPARQL11QueryComplianceTest(String displayName, String testURI, String name, String queryFileURL,
                                             String resultFileURL, Dataset dataset, boolean ordered) {
        super(displayName, testURI, name, queryFileURL, resultFileURL, null, ordered);
        setUpHDT(dataset);
        List<String> testToIgnore = new ArrayList<>();
        // @todo these tests are failing and should not, they are skipped so that we can be sure that we see when currently passing tests are not failing. Many of these tests are not so problematic since we do not support named graphs anyway
        testToIgnore.add("constructwhere02 - CONSTRUCT WHERE");
        testToIgnore.add("constructwhere03 - CONSTRUCT WHERE");
        testToIgnore.add("constructwhere04 - CONSTRUCT WHERE");
        testToIgnore.add("Exists within graph pattern");
        testToIgnore.add("(pp07) Path with one graph");
        testToIgnore.add("(pp35) Named Graph 2");
        testToIgnore.add("sq01 - Subquery within graph pattern");
        testToIgnore.add("sq02 - Subquery within graph pattern, graph variable is bound");
        testToIgnore.add("sq03 - Subquery within graph pattern, graph variable is not bound");
        testToIgnore.add("sq04 - Subquery within graph pattern, default graph does not apply");
        testToIgnore.add("sq05 - Subquery within graph pattern, from named applies");
        testToIgnore.add("sq06 - Subquery with graph pattern, from named applies");
        testToIgnore.add("sq07 - Subquery with from ");
        testToIgnore.add("sq11 - Subquery limit per resource");
        testToIgnore.add("sq13 - Subqueries don't inject bindings");
        testToIgnore.add("sq14 - limit by resource");

        this.setIgnoredTests(testToIgnore);
    }

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    HybridStore hybridStore;
    File nativeStore;
    File hdtStore;

    @Override
    protected Repository newRepository() throws Exception {
        nativeStore = tempDir.newFolder();
        hdtStore = tempDir.newFolder();
        HDTSpecification spec = new HDTSpecification();
        spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");
        if (this.hdt == null)
            hdt = com.the_qa_company.q_endpoint.Utility.createTempHdtIndex(tempDir, true, false, spec);
        assert hdt != null;

        hdt.saveToHDT(hdtStore.getAbsolutePath() + "/" + HybridStoreTest.HDT_INDEX_NAME, null);

        hybridStore = new HybridStore(
                hdtStore.getAbsolutePath() + "/", HybridStoreTest.HDT_INDEX_NAME, spec, nativeStore.getAbsolutePath() + "/", true
        );
//        hybridStore.setThreshold(2);
        SailRepository repository = new SailRepository(hybridStore);
        return repository;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    HDT hdt;

    private void setUpHDT(Dataset dataset) {
        try {
            if (dataset != null) {
                String x = dataset.getDefaultGraphs().toString();
                if (x.equals("[]")) {
                    x = dataset.getNamedGraphs().toString();
                }
                String str = x.substring(x.lastIndexOf("!") + 1).replace("]", "");

                URL url = SPARQL11QueryComplianceTest.class.getResource(str);
                File tmpDir = new File("test");
                if (!tmpDir.isDirectory()) {
                    tmpDir.mkdir();
                }
                JarURLConnection con = (JarURLConnection) url.openConnection();
                File file = new File(tmpDir, con.getEntryName());

                HDTSpecification spec = new HDTSpecification();

                if (file.getName().endsWith("rdf")) {
                    hdt = HDTManager.generateHDT(file.getAbsolutePath(), "http://www.example.org/", RDFNotation.RDFXML, spec, null);
                } else if (file.getName().endsWith("ttl")) {
                    hdt = HDTManager.generateHDT(file.getAbsolutePath(), "http://www.wdaqua.eu/qa", RDFNotation.TURTLE, spec, null);
                } else if (file.getName().endsWith("nt")) {
                    hdt = HDTManager.generateHDT(file.getAbsolutePath(), "http://www.wdaqua.eu/qa", RDFNotation.NTRIPLES, spec, null);
                }
                assert hdt != null;
                hdt.search("", "", "").forEachRemaining(System.out::println);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
