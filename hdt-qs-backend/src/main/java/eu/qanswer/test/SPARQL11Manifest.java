//package eu.qanswer.test;
//
//import junit.framework.Test;
//import org.eclipse.rdf4j.query.Dataset;
//import org.eclipse.rdf4j.query.parser.sparql.manifest.SPARQL11ManifestTest;
//import org.eclipse.rdf4j.repository.Repository;
//
//import java.net.URL;
//
//public class SPARQL11Manifest extends SPARQLQueryTestLoadBefore {
//
//    static String[] ignoredTests = {
//            // test case incompatible with RDF 1.1 - see
//            // http://lists.w3.org/Archives/Public/public-sparql-dev/2013AprJun/0006.html
//            "STRDT   TypeErrors",
//            // test case incompatible with RDF 1.1 - see
//            // http://lists.w3.org/Archives/Public/public-sparql-dev/2013AprJun/0006.html
//            "STRLANG   TypeErrors",
//
//            // known issue: SES-937
//            "sq03 - Subquery within graph pattern, graph variable is not bound",
//            //Exclude tests that need named graph
//            " pp34  Named Graph 1",
//            " pp35  Named Graph 2",
//            //"sq01 - Subquery within graph pattern",
//            //"sq02 - Subquery within graph pattern, graph variable is bound",
//            "sq04 - Subquery within graph pattern, default graph does not apply",
//            "sq05 - Subquery within graph pattern, from named applies",
//            "sq06 - Subquery with graph pattern, from named applies",
//            "sq07 - Subquery with from ",
//            " pp06  Path with two graphs",
//            "sq01 - Subquery within graph pattern",
//            "sq02 - Subquery within graph pattern, graph variable is bound",
//            " pp07  Path with one graph",
//            "Exists within graph pattern"
//
//    };
//
//    public static Test suite() throws Exception {
//        URL manifestUrl = SPARQL11ManifestTest.class.getResource("/testcases-sparql-1.1-w3c/manifest-all.ttl");
//
//
//        return SPARQL11ManifestTest.suite(new Factory() {
//
//            @Override
//            public SPARQLQueryTestLoadBefore createSPARQLQueryTest(String testURI, String name, String queryFileURL,
//                                                                      String resultFileURL, Dataset dataSet, boolean laxCardinality) {
//                return createSPARQLQueryTest(testURI, name, queryFileURL, resultFileURL, dataSet, laxCardinality,
//                        false);
//            }
//
//            @Override
//            public SPARQLQueryTestLoadBefore createSPARQLQueryTest(String testURI, String name, String queryFileURL,
//                                                                      String resultFileURL, Dataset dataSet, boolean laxCardinality, boolean checkOrder) {
//                 String[] ignoredTests = {
//                        // test case incompatible with RDF 1.1 - see
//                        // http://lists.w3.org/Archives/Public/public-sparql-dev/2013AprJun/0006.html
//                        "STRDT   TypeErrors",
//                        // test case incompatible with RDF 1.1 - see
//                        // http://lists.w3.org/Archives/Public/public-sparql-dev/2013AprJun/0006.html
//                        "STRLANG   TypeErrors",
//
//                        // known issue: SES-937
//                        "sq03 - Subquery within graph pattern, graph variable is not bound",
//                        //Exclude tests that need named graph
//                        " pp34  Named Graph 1",
//                        " pp35  Named Graph 2",
//                        //"sq01 - Subquery within graph pattern",
//                        //"sq02 - Subquery within graph pattern, graph variable is bound",
//                        "sq04 - Subquery within graph pattern, default graph does not apply",
//                        "sq05 - Subquery within graph pattern, from named applies",
//                        "sq06 - Subquery with graph pattern, from named applies",
//                        "sq07 - Subquery with from ",
//                        " pp06  Path with two graphs",
//                        "sq01 - Subquery within graph pattern",
//                        "sq02 - Subquery within graph pattern, graph variable is bound",
//                        " pp07  Path with one graph",
//                        "Exists within graph pattern"
//
//                };
//
//                return new SPARQL11Manifest(testURI, name, queryFileURL, resultFileURL, dataSet,
//                        laxCardinality, checkOrder, ignoredTests);
//            }
//        }, manifestUrl.toString(), true, "service");
//    }
//
//    protected SPARQL11Manifest(String testURI, String name, String queryFileURL, String resultFileURL,
//                               Dataset dataSet, boolean laxCardinality, boolean checkOrder, String... ignoredTests) {
//        super(testURI, name, queryFileURL, resultFileURL, dataSet, laxCardinality, checkOrder, ignoredTests);
//    }
//
//    @Override
//    protected Repository newRepository() {
//        try {
//            setUp();
//            System.out.println("Print "+this.dataRep);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return this.dataRep;
//    }
//
//}
