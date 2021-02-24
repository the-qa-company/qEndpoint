

import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.parser.sparql.manifest.SPARQL11QueryComplianceTest;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.dataset.DatasetRepository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.rdf4j.HybridStore;

import java.io.File;

/**
 * @author Ali Haidar
 *
 */
public class HybridSPARQL11QueryComplianceTest extends SPARQL11QueryComplianceTest {

    public HybridSPARQL11QueryComplianceTest(String displayName, String testURI, String name, String queryFileURL,
                                             String resultFileURL, Dataset dataset, boolean ordered) {
        super(displayName, testURI, name, queryFileURL, resultFileURL, dataset, ordered);
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
        HybridStore hybridStore = new HybridStore(
                hdtStore.getAbsolutePath()+"/",nativeStore.getAbsolutePath()+"/",true
        );
        hybridStore.setThreshold(3);

        SailRepository repository = new SailRepository(hybridStore);
        return new DatasetRepository(repository);
//        return new DatasetRepository(new SailRepository(new NativeStore(tempDir.newFolder(), "spoc")));
    }

}
