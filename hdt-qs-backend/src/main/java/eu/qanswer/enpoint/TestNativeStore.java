package eu.qanswer.enpoint;

import org.eclipse.rdf4j.http.client.RDF4JProtocolSession;
import org.eclipse.rdf4j.http.client.SharedHttpClientSessionManager;
import org.eclipse.rdf4j.http.protocol.Protocol;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.manager.RemoteRepositoryManager;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestNativeStore {

    private static final Logger logger = LoggerFactory.getLogger(TestNativeStore.class);

    public static void main(String[] args) {
        TestNativeStore nativeStore = new TestNativeStore();
        nativeStore.createProtocolSession();
    }
    private String testHeader = "X-testing-header";
    private String testValue = "foobar";

    private String serverURL = "http://localhost:1234/hdt-qs-server";
    private String repositoryID = "test";

    RDF4JProtocolSession createProtocolSession() {
        RDF4JProtocolSession session = new SharedHttpClientSessionManager().createRDF4JProtocolSession(serverURL);
        session.setRepository(Protocol.getRepositoryLocation(serverURL, repositoryID));
        HashMap<String, String> additionalHeaders = new HashMap<>();
        additionalHeaders.put(testHeader, testValue);
        session.setAdditionalHttpHeaders(additionalHeaders);
        return session;
    }

}
