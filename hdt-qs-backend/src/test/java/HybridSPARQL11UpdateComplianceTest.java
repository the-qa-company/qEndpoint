
import java.io.File;
import java.util.Map;

import eu.qanswer.hybridstore.HybridStore;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.parser.sparql.manifest.SPARQL11UpdateComplianceTest;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.dataset.DatasetRepository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.options.HDTSpecification;

/**
 * Test SPARQL 1.1 Update functionality on a native store.
 *
 * @author Ali Haidar
 */
public class HybridSPARQL11UpdateComplianceTest extends SPARQL11UpdateComplianceTest {

    public HybridSPARQL11UpdateComplianceTest(String displayName, String testURI, String name, String requestFile,
                                              IRI defaultGraphURI, Map<String, IRI> inputNamedGraphs, IRI resultDefaultGraphURI,
                                              Map<String, IRI> resultNamedGraphs) {
        super(displayName, testURI, name, requestFile, defaultGraphURI, inputNamedGraphs, resultDefaultGraphURI,
                resultNamedGraphs);
    }

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Override
    protected Repository newRepository() throws Exception {
        File nativeStore = tempDir.newFolder();
        File hdtStore = tempDir.newFolder();
        HDT hdt = Utility.createTempHdtIndex(tempDir, true,false);
        assert hdt != null;
        hdt.saveToHDT(hdtStore.getAbsolutePath()+"/index.hdt",null);

        HDTSpecification spec = new HDTSpecification();
        spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");

        HybridStore hybridStore = new HybridStore(
                hdtStore.getAbsolutePath()+"/",spec,nativeStore.getAbsolutePath()+"/",true
        );
//        hybridStore.setThreshold(2);

        SailRepository repository = new SailRepository(hybridStore);
        return repository;
//        return new DatasetRepository(new SailRepository(new NativeStore(tempDir.newFolder(), "spoc")));
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }
}