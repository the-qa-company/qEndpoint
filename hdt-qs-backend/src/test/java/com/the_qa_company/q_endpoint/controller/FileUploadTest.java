package com.the_qa_company.q_endpoint.controller;

import com.the_qa_company.q_endpoint.Application;
import com.the_qa_company.q_endpoint.controller.Sparql;
import com.the_qa_company.q_endpoint.hybridstore.HybridStore;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.FileSystemUtils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.*;

@RunWith(SpringRunner.class)
@ContextConfiguration(initializers = ConfigFileApplicationContextInitializer.class)
@SpringBootTest(classes = Application.class)
public class FileUploadTest {
    private static final Field SPARQL_MODEL;
    private static final Field SPARQL_HYBRID_STORE;

    static {
        try {
            SPARQL_MODEL = Sparql.class.getDeclaredField("model");
            SPARQL_HYBRID_STORE = Sparql.class.getDeclaredField("hybridStore");
            SPARQL_MODEL.setAccessible(true);
            SPARQL_HYBRID_STORE.setAccessible(true);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    @Autowired
    Sparql sparql;

    @Value("${locationHdt}")
    private String locationHdt;

    @Value("${hdtIndexName}")
    private String hdtIndexName;

    @Value("${locationNative}")
    private String locationNative;


    private HybridStore getHybridStore() {
        try {
            return (HybridStore) SPARQL_HYBRID_STORE.get(sparql);
        } catch (IllegalAccessException e) {
            throw new Error(e);
        }
    }

    @Before
    public void setup() throws IllegalAccessException {
        ((Map) SPARQL_MODEL.get(sparql)).clear();
        try {
            FileSystemUtils.deleteRecursively(Paths.get(locationHdt));
        } catch (IOException e) {
            //
        }
        try {
            FileSystemUtils.deleteRecursively(Paths.get(locationNative));
        } catch (IOException e) {
            //
        }
        try {
            FileSystemUtils.deleteRecursively(Paths.get(hdtIndexName));
        } catch (IOException e) {
            //
        }
    }

    private InputStream stream(String file) {
        return Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream(file), "file can't be found!");
    }

    private long fileSize(String file) throws IOException {
        InputStream testNt = stream(file);
        assert testNt != null;
        byte[] buff = new byte[1024];

        long r;
        long size = 0;
        while ((r = testNt.read(buff)) != -1) {
            size += r;
        }
        return size;
    }

    private void assertAllHDTLoaded(String file) throws ParserException, IOException, NotFoundException {
        HybridStore store = getHybridStore();
        SailRepository sailRepository = new SailRepository(store);
        List<Statement> statementList = new ArrayList<>();
        RDFParser parser = Rio.createParser(RDFFormat.NTRIPLES);
        parser.setPreserveBNodeIDs(true);
        parser.setRDFHandler(new RDFHandler() {
            @Override
            public void startRDF() throws RDFHandlerException {
            }

            @Override
            public void endRDF() throws RDFHandlerException {
            }

            @Override
            public void handleNamespace(String s, String s1) throws RDFHandlerException {
            }

            @Override
            public void handleStatement(Statement statement) throws RDFHandlerException {
                if (
                        statement.getSubject().toString().startsWith("_:")
                                || statement.getPredicate().toString().startsWith("_:")
                                || statement.getObject().toString().startsWith("_:"))
                    return; // FIXME: allow blank node usage
                statementList.add(statement);
            }

            @Override
            public void handleComment(String s) throws RDFHandlerException {

            }
        });
        parser.parse(stream(file));

        try (SailRepositoryConnection connection = sailRepository.getConnection()) {
            RepositoryResult<Statement> sts = connection.getStatements(null, null, null, false);
            while (sts.hasNext()) {
                Statement next = sts.next();
                if (
                        next.getSubject().toString().startsWith("_:")
                                || next.getPredicate().toString().startsWith("_:")
                                || next.getObject().toString().startsWith("_:"))
                    continue;
                Assert.assertTrue("Statement (" +
                        next.getSubject().toString() + ", " +
                        next.getPredicate().toString() + ", " +
                        next.getObject().toString()
                        + "), not in " + file, statementList.remove(next));
                while (statementList.remove(next))
                    ; // remove duplicates
            }
        }
        if (!statementList.isEmpty()) {
            for (Statement statement : statementList) {
                System.err.println(statement);
            }
            Assert.fail(file + "contains more triple than the HybridStore");
        }
    }

    @Test
    public void loadNoSplitTest() throws IOException, ParserException, NotFoundException {
        long size = fileSize("cocktails.nt");
        sparql.debugMaxChunkSize = size + 1;

        sparql.loadFile(stream("cocktails.nt"), "cocktails.nt");


        assertAllHDTLoaded("cocktails.nt");
    }

    @Test
    public void loadNoNTSplitTest() throws IOException, ParserException, NotFoundException {
        long size = fileSize("cocktails.nt");
        sparql.debugMaxChunkSize = size / 10;

        // use ttl file because nt c ttl to avoid split
        sparql.loadFile(stream("cocktails.nt"), "cocktails.ttl");

        assertAllHDTLoaded("cocktails.nt");
    }

    @Test
    public void loadSplitTest() throws IOException, ParserException, NotFoundException {
        long size = fileSize("cocktails.nt");
        sparql.debugMaxChunkSize = size / 10;

        sparql.loadFile(stream("cocktails.nt"), "cocktails.nt");

        assertAllHDTLoaded("cocktails.nt");
    }
}
