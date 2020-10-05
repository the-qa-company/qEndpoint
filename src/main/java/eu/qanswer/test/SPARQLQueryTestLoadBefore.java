package org.test;


import org.eclipse.rdf4j.query.Dataset;

import org.eclipse.rdf4j.query.parser.sparql.manifest.SPARQLQueryTest;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.rdf4j.HDTRepository;

import java.io.File;

import java.util.Arrays;
import java.util.Scanner;


public abstract class SPARQLQueryTestLoadBefore extends SPARQLQueryTest {

    public SPARQLQueryTestLoadBefore(String testURI, String name, String queryFileURL, String resultFileURL, Dataset dataSet, boolean laxCardinality, boolean checkOrder, String... ignoredTests){
        super(testURI, name, queryFileURL, resultFileURL, dataSet, laxCardinality, false, ignoredTests);
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

            File file = new File(this.dataset.getDefaultGraphs().toString().replace("file:", "").replace("[", "").replace("]", ""));
            HDT hdt = null;
            if(file.getName().endsWith("rdf")){
                hdt = HDTManager.generateHDT(file.getAbsolutePath(), "http://www.wdaqua.eu/qa", RDFNotation.RDFXML, new HDTSpecification(), null);
            }else if(file.getName().endsWith("ttl")){
                hdt = HDTManager.generateHDT(file.getAbsolutePath(), "http://www.wdaqua.eu/qa", RDFNotation.TURTLE, new HDTSpecification(), null);
            }else if(file.getName().endsWith("nt")){
                hdt = HDTManager.generateHDT(file.getAbsolutePath(), "http://www.wdaqua.eu/qa", RDFNotation.NTRIPLES, new HDTSpecification(), null);
            }
            this.dataRep = new HDTRepository(hdt);
            this.dataRep.init();
            System.out.println("Query " + this.readQueryString());
        }

    }
}