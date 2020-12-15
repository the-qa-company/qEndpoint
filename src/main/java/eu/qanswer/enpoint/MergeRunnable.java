package eu.qanswer.enpoint;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.rdf4j.HybridQueryPreparer;
import org.rdfhdt.hdt.rdf4j.HybridStore;
import org.rdfhdt.hdt.rdf4j.HybridTripleSource;
import org.rdfhdt.hdt.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class MergeRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(MergeRunnable.class);
    private RepositoryConnection nativeStoreConnection;
    private String hdtIndex;
    private String locationHdt;

    private HDT hdt;
    private HybridStore hybridStore;
    public MergeRunnable(HDT hdt,RepositoryConnection nativeStoreConnection) {
        //this.hdt = hdt;
        this.nativeStoreConnection = nativeStoreConnection;
    }
    public MergeRunnable(String locationHdt,RepositoryConnection nativeStoreConnection) {
        this.locationHdt = locationHdt;
        this.hdtIndex = locationHdt+"index.hdt";
        this.nativeStoreConnection = nativeStoreConnection;
    }

    public MergeRunnable(String locationHdt, RepositoryConnection nativeStoreConnection, HybridStore hybridStore) {
        this.locationHdt = locationHdt;
        this.hdtIndex = locationHdt+"index.hdt";
        this.nativeStoreConnection = nativeStoreConnection;
        this.hybridStore = hybridStore;
    }


    public synchronized void run() {
        hybridStore.isMerging = true;
        try {
            // dump all triples in native store
            writeTempFile();
            // create the hdt index for the temp dump
            createHDTDump();
            // cat the original index and the temp index
            catIndexes();
            // empty native store
            emptyNativeStore();
            Files.delete(Paths.get(locationHdt+"/temp.hdt"));
            Files.delete(Paths.get(locationHdt+"/temp.nt"));

            this.hdt = HDTManager.mapIndexedHDT(hdtIndex,new HDTSpecification());
            this.hybridStore.setTripleSource(new HybridTripleSource(hdt,this.hybridStore));
            this.hybridStore.setQueryPreparer(new HybridQueryPreparer(this.hybridStore));
            this.hybridStore.setHdt(this.hdt);
            this.hybridStore.isMerging = false;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private void catIndexes() throws IOException {
        String hdtOutput = locationHdt+"new_index.hdt";
        String hdtInput1 = hdtIndex;
        String hdtInput2 = locationHdt+"temp.hdt";
        HDT hdt = null;
        HDTSpecification spec = new HDTSpecification();
        try {
            File file = new File(hdtOutput);
            File theDir = new File(file.getAbsolutePath()+"_tmp");
            theDir.mkdirs();
            String location = theDir.getAbsolutePath()+"/";
            hdt = HDTManager.catHDT(location,hdtInput1, hdtInput2 , spec,null);

            StopWatch sw = new StopWatch();
            hdt.saveToHDT(hdtOutput, null);
            logger.info("HDT saved to file in: "+sw.stopAndShow());
            Files.delete(Paths.get(location+"dictionary"));
            Files.delete(Paths.get(location+"triples"));
            theDir.delete();

            hdt = HDTManager.indexedHDT(hdt,null);

            Files.deleteIfExists(Paths.get(hdtIndex));
            Files.deleteIfExists(Paths.get(hdtIndex+".index.v1-1"));
            boolean renamed = file.renameTo(new File(hdtIndex));
            File indexFile = new File(hdtOutput+".index.v1-1");
            boolean renamedIndex = indexFile.renameTo(new File(hdtIndex+".index.v1-1"));
            if(renamed && renamedIndex) {
                logger.info("Replaced indexes successfully");
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(hdt != null)
                hdt.close();
        }

    }
    private void createHDTDump(){

        String rdfInput = locationHdt+"temp.nt";
        String hdtOutput = locationHdt+"temp.hdt";
        String baseURI = "file://"+rdfInput;
        RDFNotation notation = RDFNotation.guess(rdfInput);
        HDTSpecification spec = new HDTSpecification();

        try {
            StopWatch sw = new StopWatch();
            HDT hdt = HDTManager.generateHDT(rdfInput, baseURI,notation , spec, null);
            logger.info("File converted in: "+sw.stopAndShow());
            hdt.saveToHDT(hdtOutput, null);
            logger.info("HDT saved to file in: "+sw.stopAndShow());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParserException e) {
            e.printStackTrace();
        }
    }
    private void writeTempFile(){
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(locationHdt+"temp.nt");
            RDFWriter writer = Rio.createWriter(RDFFormat.NTRIPLES, out);
            RepositoryResult<Statement> repositoryResult =
                    this.nativeStoreConnection.getStatements(null,null,null,false,(Resource)null);
            writer.startRDF();
            while (repositoryResult.hasNext()) {
                Statement stm = repositoryResult.next();
                writer.handleStatement(stm);
            }
            writer.endRDF();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private void emptyNativeStore(){
        nativeStoreConnection.clear((Resource)null);
    }
}