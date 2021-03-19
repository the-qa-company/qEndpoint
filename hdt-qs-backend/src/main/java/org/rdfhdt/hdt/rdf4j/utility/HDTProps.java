package org.rdfhdt.hdt.rdf4j.utility;

import org.rdfhdt.hdt.hdt.HDT;

public class HDTProps {

    private final long startLiteral;
    private final long endLiteral;
    private final long startBlank;
    private final long endBlank;

    public HDTProps(HDT hdt){

        this.startLiteral =
                BinarySearch.first(
                        hdt.getDictionary(),
                        hdt.getDictionary().getNshared() + 1,
                        hdt.getDictionary().getNobjects(),
                        "\"");
        this.endLiteral =
                BinarySearch.last(
                        hdt.getDictionary(),
                        hdt.getDictionary().getNshared() + 1,
                        hdt.getDictionary().getNobjects(),
                        hdt.getDictionary().getNobjects(),
                        "\"");

        this.startBlank = BinarySearch.first(
                hdt.getDictionary(),
                hdt.getDictionary().getNshared() + 1,
                hdt.getDictionary().getNobjects(),
                "_");
        this.endBlank = BinarySearch.last(
                hdt.getDictionary(),
                hdt.getDictionary().getNshared() + 1,
                hdt.getDictionary().getNobjects(),
                hdt.getDictionary().getNobjects(),
                "_");
    }

    public long getEndLiteral() {
        return endLiteral;
    }

    public long getStartLiteral() {
        return startLiteral;
    }

    public long getEndBlank() {
        return endBlank;
    }

    public long getStartBlank() {
        return startBlank;
    }
}
