package eu.qanswer.enpoint;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.explanation.Explanation;
import org.eclipse.rdf4j.query.parser.*;
import org.eclipse.rdf4j.query.resultio.sparqljson.SPARQLResultsJSONWriter;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.lucene.HDTLuceneSail;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.rdf4j.HybridStore;
import org.rdfhdt.hdt.rdf4j.misc.HDTStore;
import org.rdfhdt.hdt.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

@Component
public class Sparql {
    private static final Logger logger = LoggerFactory.getLogger(Sparql.class);
    private final HashMap<String, RepositoryConnection> model = new HashMap<>();

    private final int THRESHOLD = 5;
    private NativeStore nativeStore;
    @Value("${locationHdt}")
    private String locationHdt;
    @Value("${locationNativeA}")
    private String locationNativeA;
    @Value("${locationNativeB}")
    private String locationNativeB;

    private String hdtindex = "index.hdt";
    void initialize(String location) throws Exception {
        if (!model.containsKey(location)) {
            model.put(location, null);
            System.out.println("initialize "+location);
            HDTSpecification spec = new HDTSpecification();
            //spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");

            HDT hdt = HDTManager.mapIndexedHDT(
                            new File(location+"index.hdt").getAbsolutePath(),spec);

            HDTStore hdtStore = new HDTStore(hdt,this.nativeStore);
            LuceneSail luceneSail = new LuceneSail();
            luceneSail.setReindexQuery("select ?s ?p ?o where {?s ?p ?o}");
            luceneSail.setParameter(LuceneSail.LUCENE_DIR_KEY, location + "/lucene");
            luceneSail.setParameter(LuceneSail.WKT_FIELDS, "http://nuts.de/geometry");
            luceneSail.setBaseSail(hdtStore);
            luceneSail.initialize();
            //lucenesail.reindex();
            Repository db = new SailRepository(luceneSail);
            db.init();
            RepositoryConnection conn = db.getConnection();
            model.put(location, conn);
        }
    }
    NativeStore nativeStoreA;
    NativeStore nativeStoreB;
    public void initializeNativeStore(){
        if(!model.containsKey("native-store")) {
            File dataDir1 = new File(locationNativeA);
            File dataDir2 = new File(locationNativeB);
            String indexes = "spoc,posc,cosp";
            nativeStoreA = new NativeStore(dataDir1,indexes);
            nativeStoreB = new NativeStore(dataDir2,indexes);

            this.nativeStore = nativeStoreA;
            Repository repo = new SailRepository(this.nativeStore);
            RepositoryConnection connection = repo.getConnection();
            model.put("native-store", connection);
        }
    }
    public String executeJson(String sparqlQuery, int timeout) throws Exception {
        logger.info("Json " + sparqlQuery);
        initializeNativeStore();
        initialize(locationHdt);

        ParsedQuery parsedQuery =
                QueryParserUtil.parseQuery(QueryLanguage.SPARQL, sparqlQuery, null);

        if (parsedQuery instanceof ParsedTupleQuery) {
            TupleQuery query = model.get(locationHdt).prepareTupleQuery(sparqlQuery);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            TupleQueryResultHandler writer = new SPARQLResultsJSONWriter(out);
            query.setMaxExecutionTime(timeout);
            try {
                query.evaluate(writer);
            } catch (QueryEvaluationException q){
                System.out.println("This exception was caught ["+q+"]");
            }
            return out.toString("UTF8");
        } else if (parsedQuery instanceof ParsedBooleanQuery) {
            BooleanQuery query = model.get(locationHdt).prepareBooleanQuery(sparqlQuery);
            if (query.evaluate() == true) {
                return "{ \"head\" : { } , \"boolean\" : true }";
            } else {
                return "{ \"head\" : { } , \"boolean\" : false }";
            }
        } else {
            System.out.println("Not knowledge-base yet: query is neither a SELECT nor an ASK");
            return "Bad Request : query not supported ";
        }
    }
    public int getCurrentCount(){
        initializeNativeStore();
        String queryCount = "select (count(*) as ?c) where { ?s ?p ?o}";

        TupleQuery tupleQuery = model.get("native-store").prepareTupleQuery(queryCount);
        try (TupleQueryResult result = tupleQuery.evaluate()) {
            while (result.hasNext()) {
                BindingSet bindingSet = result.next();
                org.eclipse.rdf4j.model.Value valueOfC = bindingSet.getValue("c");
                return Integer.parseInt(valueOfC.stringValue());
            }
        }
        return 0;
    }
    boolean switchStore = true;
    public String executeUpdate(String sparqlQuery, int timeout) throws Exception {
        initializeNativeStore();
        logger.info("Running update query:"+sparqlQuery);
        int count = getCurrentCount();
        if(count >= THRESHOLD){
            // switch native store - cat hdt with the index

            makeMerge();
            if(switchStore){
                this.nativeStore = nativeStoreB;
                switchStore = false;
            }else{
                this.nativeStore = nativeStoreA;
                switchStore = true;
            }
            // replace the connection with the switched store
            Repository repo = new SailRepository(this.nativeStore);
            RepositoryConnection connection = repo.getConnection();
            model.put("native-store", connection);
        }
        Update preparedUpdate = model.get("native-store").prepareUpdate(QueryLanguage.SPARQL,sparqlQuery);
        if(preparedUpdate != null) {
            preparedUpdate.execute();
            return "OK\n";
        }
        return null;
    }

    public String makeMerge() {
        initializeNativeStore();
        HDTSpecification spec = new HDTSpecification();
        try {
            HDT hdt = HDTManager.mapIndexedHDT(
                    new File(locationHdt+"index.hdt").getAbsolutePath(),spec);
            MergeThread mergeThread = new MergeThread(locationHdt+"/"+hdtindex,model.get("native-store"));
            mergeThread.run();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "Merged!";
    }

    public class MergeThread implements Runnable {

        private HDT hdt;
        private RepositoryConnection nativeStoreConnection;
        private String hdtIndex;
        public MergeThread(HDT hdt,RepositoryConnection nativeStoreConnection) {
            this.hdt = hdt;
            this.nativeStoreConnection = nativeStoreConnection;
        }
        public MergeThread(String hdtIndex,RepositoryConnection nativeStoreConnection) {
            this.hdtIndex = hdtIndex;
            this.nativeStoreConnection = nativeStoreConnection;
        }


        public void run() {
            // dump all triples in native store
            writeTempFile();
            // create the hdt index for the temp dump
            createHDTDump();
            // cat the original index and the temp index
            catIndexes();
            // empty native store
            emptyNativeStore();
            try {
                Files.delete(Paths.get(locationHdt+"/temp.hdt"));
                Files.delete(Paths.get(locationHdt+"/temp.nt"));

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        private void catIndexes(){

            String hdtOutput = locationHdt+"index.hdt";
            String hdtInput1 = hdtIndex;
            String hdtInput2 = locationHdt+"/temp.hdt";
            HDTSpecification spec = new HDTSpecification();
            try {
                File file = new File(hdtOutput);
                File theDir = new File(file.getAbsolutePath()+"_tmp");
                theDir.mkdirs();
                String location = theDir.getAbsolutePath()+"/";
                HDT hdt = HDTManager.catHDT(location,hdtInput1, hdtInput2 , spec,null);

                StopWatch sw = new StopWatch();
                hdt.saveToHDT(hdtOutput, null);
                System.out.println("HDT saved to file in: "+sw.stopAndShow());
                Files.delete(Paths.get(location+"dictionary"));
                Files.delete(Paths.get(location+"triples"));
                theDir.delete();
            } catch (IOException e) {
                e.printStackTrace();
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
                System.out.println("File converted in: "+sw.stopAndShow());
                hdt.saveToHDT(hdtOutput, null);
                System.out.println("HDT saved to file in: "+sw.stopAndShow());
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
}
