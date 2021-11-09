//package eu.qanswer;
//
//import com.metreeca.jse.JSEServer;
//import com.metreeca.rdf4j.assets.Graph;
//import eu.qanswer.hybridstore.Sparql;
//import org.eclipse.rdf4j.repository.sail.SailRepository;
//import org.eclipse.rdf4j.sail.evaluation.TupleFunctionEvaluationMode;
//import org.eclipse.rdf4j.sail.lucene.LuceneSail;
//import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
//import org.rdfhdt.hdt.enums.RDFNotation;
//import org.rdfhdt.hdt.exceptions.ParserException;
//import org.rdfhdt.hdt.hdt.HDT;
//import org.rdfhdt.hdt.hdt.HDTManager;
//import org.rdfhdt.hdt.options.HDTSpecification;
//import org.rdfhdt.hdt.rdf4j.HybridStore;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Value;
//
//import java.io.File;
//import java.io.IOException;
//import java.io.UncheckedIOException;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//
//import static com.metreeca.rdf4j.assets.Graph.graph;
//import static com.metreeca.rdf4j.handlers.Graphs.graphs;
//import static com.metreeca.rdf4j.handlers.SPARQL.sparql;
//import static com.metreeca.rest.Context.asset;
//import static com.metreeca.rest.handlers.Router.router;
//import static com.metreeca.rest.wrappers.CORS.cors;
//import static com.metreeca.rest.wrappers.Gateway.gateway;
//
//public class RDF4JServer {
//    private static final Logger logger = LoggerFactory.getLogger(RDF4JServer.class);
//    public static SailRepository initializeRepository(){
//
//        HybridStore hybridStore;
//        LuceneSail luceneSail;
//        SailRepository repository;
//        String locationHdt = "./hdt-store/";
//        String locationNative = "./native-store/";
//        int threshold = 10000;
//        HDTSpecification spec = new HDTSpecification();
//        //spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");
//
//        File hdtFile = new File(locationHdt+"index.hdt");
//        if(!hdtFile.exists()){
//            File tempRDF = new File(locationHdt+"tmp_index.nt");
//            try {
//                if(tempRDF.createNewFile()){
//                    logger.info("Empty HDT index created");
//                }
//                HDT hdt = HDTManager.generateHDT(tempRDF.getAbsolutePath(),"uri", RDFNotation.NTRIPLES,spec,null);
//                hdt.saveToHDT(hdtFile.getPath(),null);
//                Files.delete(Paths.get(tempRDF.getAbsolutePath()));
//            } catch (IOException | ParserException e) {
//                e.printStackTrace();
//            }
//        }
//
//        hybridStore = new HybridStore(locationHdt,locationNative,true);
//        hybridStore.setThreshold(threshold);
//        logger.info("Threshold for triples in Native RDF store: "+threshold+" triples");
//        luceneSail = new LuceneSail();
//        luceneSail.setReindexQuery("select ?s ?p ?o where {?s ?p ?o}");
//        luceneSail.setParameter(LuceneSail.LUCENE_DIR_KEY, locationHdt + "/lucene");
//        luceneSail.setParameter(LuceneSail.WKT_FIELDS, "http://nuts.de/geometry");
//        luceneSail.setBaseSail(hybridStore);
//        luceneSail.setEvaluationMode(TupleFunctionEvaluationMode.NATIVE);
//        luceneSail.initialize();
//        repository = new SailRepository(luceneSail);
//        repository.init();
//        //lucenesail.reindex();
//        return repository;
//    }
//    public static void main(final String... args) {
//        new JSEServer()
//
//                .delegate(context -> context
//
//                        .set(graph(), () -> new Graph(initializeRepository()))
//
//                        .exec(() -> asset(graph()).exec(connection -> {
//
//                            // load repository content here using connection
//
//                        }))
//                        .get(() -> gateway()
//
//                                .with(cors())
//
//                                .wrap(router()
//
////                                        .path("/sparql", sparql().query())
//                                        .path("/graphs", graphs())
//                                        .path("/sparql",sparql().update())
//
//                                )
//
//                        )
//                )
//
//                .start();
//    }
//
//}
