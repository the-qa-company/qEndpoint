package com.the_qa_company.q_endpoint.hybridstore;

import com.the_qa_company.q_endpoint.utils.BinarySearch;

import org.rdfhdt.hdt.dictionary.impl.MultipleSectionDictionary;
import org.rdfhdt.hdt.hdt.HDT;

public class HDTProps {

    private final long startLiteral;
    private final long endLiteral;
    private final long startBlankObjects;
    private final long endBlankObjects;
    private final long startBlankShared;
    private final long endBlankShared;

    public HDTProps(HDT hdt) {

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
        long start;
        long end;
        // if the dictionay is spliting the objects to sections - we just have to look in the
        // NO_DATATYPE section for the blank nodes range
        if (hdt.getDictionary() instanceof MultipleSectionDictionary) {
            MultipleSectionDictionary dictionary = (MultipleSectionDictionary) hdt.getDictionary();
            start = dictionary.getDataTypeRange("NO_DATATYPE").getKey();
            end = dictionary.getDataTypeRange("NO_DATATYPE").getValue();
            if (end == 0) {
                end = -1;
            }
        } else {
            // other wise we look over the whole objects section
            start = hdt.getDictionary().getNshared() + 1;
            end = hdt.getDictionary().getNobjects();
        }
        // use binary search to check the start and the end of blank nodes
        // where items are sorted lexicographically
        this.startBlankObjects = BinarySearch.first(
                hdt.getDictionary(),
                start,
                end,
                "_");
        this.endBlankObjects = BinarySearch.last(
                hdt.getDictionary(),
                start,
                end,
                end,
                "_");

        // we need as well the range of the blank nodes in the shared section of the dictionary
        // in order to distinguish them from URIs at query time. (could be used in inserts and deletes as well)
        // like INSERT { ?s1 rdfs:type ex:Person } where { ?s rdfs:label "Ali" . }

        this.startBlankShared = BinarySearch.first(hdt.getDictionary(), 1, hdt.getDictionary().getShared().getNumberOfElements(), "_");
        this.endBlankShared = BinarySearch.last(
                hdt.getDictionary(),
                1,
                hdt.getDictionary().getShared().getNumberOfElements(),
                hdt.getDictionary().getShared().getNumberOfElements(),
                "_");

    }

    public long getEndLiteral() {
        return endLiteral;
    }

    public long getStartLiteral() {
        return startLiteral;
    }

    public long getEndBlankShared() {
        return endBlankShared;
    }

    public long getStartBlankShared() {
        return startBlankShared;
    }

    public long getEndBlankObjects() {
        return endBlankObjects;
    }

    public long getStartBlankObjects() {
        return startBlankObjects;
    }
}
