package eu.qanswer.test;


import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.parser.sparql.manifest.SPARQLQueryTest;
import org.eclipse.rdf4j.repository.Repository;

import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;

import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import java.io.*;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.Arrays;


public abstract class SPARQLQueryTestLoadBefore extends SPARQLQueryTest {

    public SPARQLQueryTestLoadBefore(String testURI, String name, String queryFileURL, String resultFileURL, Dataset dataSet, boolean laxCardinality, boolean checkOrder, String... ignoredTests){
        super(testURI, name, queryFileURL, resultFileURL, dataSet, laxCardinality, false, ignoredTests);
    }
    Repository nativeRepo;

    @Override
    protected void tearDown() throws Exception {
        nativeRepo.shutDown();
        super.tearDown();
    }

    @Override
    protected void setUp() throws Exception {
        System.out.println(this.getName());
        for (String s: this.ignoredTests) {
            System.out.println(s);
        }
        if (Arrays.asList(this.ignoredTests).contains(this.getName())) {
            this.logger.warn("Query test ignored: " + this.getName());
        } else {
            System.out.println("This dataset here " + this.dataset.getDefaultGraphs());
            System.out.println("Reading1 ");


            String x = this.dataset.getDefaultGraphs().toString();
            String str = x.substring(x.indexOf("!") +1 ).replace("]","");

            URL url = SPARQL11Manifest.class.getResource(str);
            //File tmpDir = FileUtil.createTempDir("sparql11-test-evaluation");
            File tmpDir = new File("test");
            if(!tmpDir.isDirectory()){
                tmpDir.mkdir();
            }
            JarURLConnection con = (JarURLConnection)url.openConnection();
            //JarFile jar = con.getJarFile();
            //ZipUtil.extract(jar, tmpDir);
            File file = new File(tmpDir, con.getEntryName());

            HDT hdt = null;

            HDTSpecification spec = new HDTSpecification();
            spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj");

            if(file.getName().endsWith("rdf")){
                hdt = HDTManager.generateHDT(file.getAbsolutePath(), "http://www.wdaqua.eu/qa", RDFNotation.RDFXML, spec, null);
            }else if(file.getName().endsWith("ttl")){
                hdt = HDTManager.generateHDT(file.getAbsolutePath(), "http://www.wdaqua.eu/qa", RDFNotation.TURTLE, spec, null);
            }else if(file.getName().endsWith("nt")){
                hdt = HDTManager.generateHDT(file.getAbsolutePath(), "http://www.wdaqua.eu/qa", RDFNotation.NTRIPLES, spec, null);
            }
            //this is working
            //this.dataRep = new SailRepository(new HDTStore(hdt));
            //this.dataRep.init();

            MemoryStore baseSail = new MemoryStore();
            baseSail.initialize();
            LuceneSail lucenesail = new LuceneSail();
            //lucenesail.setReindexQuery("SELECT ?s ?p ?o WHERE { {SELECT ?s ?p ?o WHERE {?s ?p ?o . FILTER (?p=<https://linkedopendata.eu/prop/direct/P836>)} } UNION {SELECT ?s ?p ?o WHERE {?s ?p ?o . ?s <https://linkedopendata.eu/prop/direct/P35> <https://linkedopendata.eu/entity/Q196899> . FILTER (?p=<http://www.w3.org/2000/01/rdf-schema#label>)} } UNION {SELECT ?s ?p ?o WHERE {?s ?p ?o . ?s <https://linkedopendata.eu/prop/direct/P35> <https://linkedopendata.eu/entity/Q9934> . FILTER (?p=<http://www.w3.org/2000/01/rdf-schema#label>) }} UNION {SELECT ?s ?p ?o WHERE {?s ?p ?o . FILTER (?p = <https://linkedopendata.eu/prop/direct/P127>) } } } order by ?s");
            lucenesail.setReindexQuery("select ?s ?p ?o where {?s ?p ?o}");
            lucenesail.setParameter(LuceneSail.LUCENE_DIR_KEY, "/Users/Dennis/Downloads/lucene_test");
            lucenesail.setParameter(LuceneSail.WKT_FIELDS, "http://nuts.de/geometry");
            lucenesail.setBaseSail(baseSail);
            lucenesail.initialize();
            lucenesail.reindex();

            this.dataRep = new SailRepository(lucenesail);
        }

    }
}