package com.the_qa_company.q_endpoint.utils;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.*;
import org.rdfhdt.hdt.triples.TripleString;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.function.Consumer;

/**
 * Utility class to help to handle RDF stream
 * @author Antoine Willerval
 */
public class RDFStreamUtils {
    /**
     * read a stream of a certain type to {@link Statement}
     * @param stream rdf stream
     * @param format format of the stream
     * @param keepBNode keep blank node
     * @param statementConsumer the triple consumer
     * @throws IOException io error
     */
    public static void readRDFStream(InputStream stream, RDFFormat format, boolean keepBNode, Consumer<Statement> statementConsumer) throws IOException {
        RDFParser parser = Rio.createParser(format);
        parser.setPreserveBNodeIDs(keepBNode);
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
                statementConsumer.accept(statement);
            }

            @Override
            public void handleComment(String s) throws RDFHandlerException {

            }
        });
        parser.parse(stream);
    }

    /**
     * create an iterator from a RDF inputstream
     * @param stream rdf stream
     * @param format format of the stream
     * @param keepBNode keep blank node
     * @return the iterator
     */
    public static Iterator<Statement> readRDFStreamAsIterator(InputStream stream, RDFFormat format, boolean keepBNode) {
        return PipedIterator.createOfCallback(pipe -> readRDFStream(stream, format, keepBNode, pipe::addElement));
    }

    /**
     * create an iterator from a RDF inputstream
     * @param stream rdf stream
     * @param format format of the stream
     * @param keepBNode keep blank node
     * @return the iterator
     */
    public static Iterator<TripleString> readRDFStreamAsTripleStringIterator(InputStream stream, RDFFormat format, boolean keepBNode) {
        return new MapIterator<>(readRDFStreamAsIterator(stream, format, keepBNode), statement -> new TripleString(
                statement.getSubject().toString(),
                statement.getPredicate().toString(),
                statement.getObject().toString()
        ));
    }
    private RDFStreamUtils() {}
}
