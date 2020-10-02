package org.eclipse.rdf4j.model.impl.util;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;

import static org.eclipse.rdf4j.rio.ntriples.NTriplesUtil.parseURI;
import static org.eclipse.rdf4j.rio.ntriples.NTriplesUtil.unescapeString;

public class LiteralParser {

    public static Literal parseLiteral(String nTriplesLiteral, ValueFactory valueFactory) throws IllegalArgumentException {
        if (nTriplesLiteral.startsWith("\"")) {
            int endLabelIdx = findEndOfLabel(nTriplesLiteral);
            if (endLabelIdx != -1) {
                int startLangIdx = nTriplesLiteral.indexOf(64, endLabelIdx);
                int startDtIdx = nTriplesLiteral.indexOf("^^", endLabelIdx);
                if (startLangIdx != -1 && startDtIdx != -1) {
                    throw new IllegalArgumentException("Literals can not have both a language and a datatype");
                }

                String label = nTriplesLiteral.substring(1, endLabelIdx);
                label = unescapeString(label);
                String datatype;
                if (startLangIdx != -1) {
                    datatype = nTriplesLiteral.substring(startLangIdx + 1);
                    return valueFactory.createLiteral(label, datatype);
                }

                if (startDtIdx != -1) {
                    datatype = nTriplesLiteral.substring(startDtIdx + 2);
                    IRI dtURI = parseURI(datatype, valueFactory);
                    return valueFactory.createLiteral(label, dtURI);
                }

                return valueFactory.createLiteral(label);
            }
        }
        throw new IllegalArgumentException("Not a legal N-Triples literal: " + nTriplesLiteral);
    }

    private static int findEndOfLabel(String nTriplesLiteral) {
        for(int i = nTriplesLiteral.length()-1; i>=0; --i) {
            char c = nTriplesLiteral.charAt(i);
            if (c == '"') {
                return i;
            }
        }
        return -1;
    }


}
