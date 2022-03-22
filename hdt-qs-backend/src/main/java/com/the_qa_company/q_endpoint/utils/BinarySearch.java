package com.the_qa_company.q_endpoint.utils;

import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.util.string.CharSequenceComparator;

// Small util to find literals in the dictionary using binary search

public class BinarySearch {
    private static final CharSequenceComparator COMPARATOR = new CharSequenceComparator();

    // FIRST occurrence of " in the dictionary between the indexes low and high, if not found return
    // -1
    public static long first(Dictionary dictionary, long low, long high, String string) {
        if (high >= low) {
            long mid = low + (high - low) / 2;
            int c = -1;
            if (mid != 1) {
                String s = dictionary.idToString(mid - 1, TripleComponentRole.OBJECT).toString();
                c = COMPARATOR.compare(string, s.substring(0, Math.min(s.length(), string.length())));
            }
            String s = dictionary.idToString(mid, TripleComponentRole.OBJECT).toString();
            int c2 = COMPARATOR.compare(string, s.substring(0, Math.min(s.length(), string.length())));
            if ((mid == 1 || c != 0) && c2 == 0) return mid;
            else if (c > 0) return first(dictionary, (mid + 1), high, string);
            else return first(dictionary, low, (mid - 1), string);
        }
        return -1;
    }

    // LAST occurrence of " in the dictionary between the indexes low and high, if not found return -1
    public static long last(Dictionary dictionary, long low, long high, long n, String string) {
        if (high >= low) {
            long mid = low + (high - low) / 2;
            int c = -1;
            if (mid != n) {
                c =
                        COMPARATOR.compare(
                                string,
                                dictionary
                                        .idToString(mid + 1, TripleComponentRole.OBJECT)
                                        .toString()
                                        .subSequence(0, 1));
            }
            int c2 =
                    COMPARATOR.compare(
                            string,
                            dictionary.idToString(mid, TripleComponentRole.OBJECT).toString().subSequence(0, 1));
            //            System.out.println("c"+c);
            //            System.out.println("c2 "+c2);
            if ((mid == n || c != 0) && c2 == 0) return mid;
            else if (c < 0) return last(dictionary, low, (mid - 1), n, string);
            else return last(dictionary, (mid + 1), high, n, string);
        }
        return -1;
    }
}
