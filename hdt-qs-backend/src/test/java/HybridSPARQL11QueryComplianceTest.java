

import eu.qanswer.hybridstore.HybridStore;
import org.eclipse.rdf4j.common.iteration.Iterations;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.parser.sparql.manifest.SPARQL11QueryComplianceTest;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.dataset.DatasetRepository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;

import java.io.File;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.List;

/**
 * @author Ali Haidar
 *
 */
public class HybridSPARQL11QueryComplianceTest extends SPARQL11QueryComplianceTest {

    public HybridSPARQL11QueryComplianceTest(String displayName, String testURI, String name, String queryFileURL,
                                             String resultFileURL, Dataset dataset, boolean ordered) {
        super(displayName, testURI, name, queryFileURL, resultFileURL, null, ordered);
        setUpHDT(dataset);
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
        if(this.hdt == null)
            hdt = Utility.createTempHdtIndex(tempDir, true,false);
        assert hdt != null;
        HDTSpecification spec = new HDTSpecification();
        spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");
        hdt.saveToHDT(hdtStore.getAbsolutePath()+"/index.hdt",null);

        hybridStore = new HybridStore(
                hdtStore.getAbsolutePath()+"/",spec,nativeStore.getAbsolutePath()+"/",true
        );
//        hybridStore.setThreshold(2);
        SailRepository repository = new SailRepository(hybridStore);
        return repository;
//        return new DatasetRepository(repository);
//        return new DatasetRepository(new SailRepository(new NativeStore(tempDir.newFolder(), "spoc")));
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
//        this.hybridStore.makeMerge();
//        RepositoryConnection connection = this.getDataRepository().getConnection();
//        connection.clear();
//        connection.close();
    }

    HDT hdt;
    private void setUpHDT(Dataset dataset) {
        try {
            if(dataset != null) {
                String x = dataset.getDefaultGraphs().toString();
                if(x.equals("[]")){
                    x = dataset.getNamedGraphs().toString();
                }
                String str = x.substring(x.lastIndexOf("!") + 1).replace("]", "");

                URL url = SPARQL11QueryComplianceTest.class.getResource(str);
                //File tmpDir = FileUtil.createTempDir("sparql11-test-evaluation");
                File tmpDir = new File("test");
                if (!tmpDir.isDirectory()) {
                    tmpDir.mkdir();
                }
                JarURLConnection con = (JarURLConnection) url.openConnection();
                //JarFile jar = con.getJarFile();
                //ZipUtil.extract(jar, tmpDir);
                File file = new File(tmpDir, con.getEntryName());


                HDTSpecification spec = new HDTSpecification();
                //spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj");

                if (file.getName().endsWith("rdf")) {
                    hdt = HDTManager.generateHDT(file.getAbsolutePath(), "http://www.example.org/", RDFNotation.RDFXML, spec, null);
                } else if (file.getName().endsWith("ttl")) {
                    hdt = HDTManager.generateHDT(file.getAbsolutePath(), "http://www.wdaqua.eu/qa", RDFNotation.TURTLE, spec, null);
                } else if (file.getName().endsWith("nt")) {
                    hdt = HDTManager.generateHDT(file.getAbsolutePath(), "http://www.wdaqua.eu/qa", RDFNotation.NTRIPLES, spec, null);
                }
                assert hdt != null;
                hdt.search("", "", "").forEachRemaining(System.out::println);
                //hdt.getDictionary().getObjects().getSortedEntries().forEachRemaining(System.out::println);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
