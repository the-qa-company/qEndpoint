package com.the_qa_company.qendpoint.utils;

import org.eclipse.rdf4j.query.resultio.QueryResultFormat;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultWriterRegistry;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;

import java.util.Optional;

/**
 * Utility class to get response format
 *
 * @author Antoine Willerval
 */
public class FormatUtils {

    /**
     * get a result writer format for an accept header
     *
     * @param acceptHeader
     *            the format to search
     *
     * @return the format
     */
    public static Optional<QueryResultFormat> getResultWriterFormat(String acceptHeader) {
        for (String mime : acceptHeader.split("[,;]")) {
            Optional<QueryResultFormat> format = TupleQueryResultWriterRegistry.getInstance()
                    .getFileFormatForMIMEType(mime);
            if (format.isPresent()) {
                return format;
            }
        }
        return Optional.empty();
    }

    /**
     * get a result writer format for an accept header
     *
     * @param acceptHeader
     *            the format to search
     *
     * @return the format
     */
    public static Optional<RDFFormat> getRDFWriterFormat(String acceptHeader) {
        for (String mime : acceptHeader.split("[,;]")) {
            Optional<RDFFormat> format = Rio.getWriterFormatForMIMEType(mime);
            if (format.isPresent()) {
                return format;
            }
        }
        return Optional.empty();
    }
}
