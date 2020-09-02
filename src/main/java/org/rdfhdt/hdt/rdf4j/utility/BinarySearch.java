package org.rdfhdt.hdt.rdf4j.utility;

import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.util.string.CharSequenceComparator;

// Small util to find literals in the dictionary using binary search

public class BinarySearch {
  private static CharSequenceComparator comparator = new CharSequenceComparator();

  // FIRST occurrence of " in the dictionary between the indexes low and high, if not found return
  // -1
  public static long first(Dictionary dictionary, long low, long high, String string) {
    //        System.out.println(low+"--"+high);
    if (high >= low) {
      long mid = low + (high - low) / 2;
      //            System.out.println("low "+low + " " +  dictionary.idToString(low,
      // TripleComponentRole.OBJECT).toString());
      //            System.out.println("high "+high + " " +  dictionary.idToString(high,
      // TripleComponentRole.OBJECT).toString());
      //            System.out.println("mid "+mid + " " +  dictionary.idToString(mid,
      // TripleComponentRole.OBJECT).toString());
      int c = -1;
      if (mid != 1) {
        //                System.out.println("mid-1"+(mid-1)+dictionary.idToString(mid - 1,
        // TripleComponentRole.OBJECT).toString());
        String s = dictionary.idToString(mid - 1, TripleComponentRole.OBJECT).toString();
        c = comparator.compare(string, s.substring(0, Math.min(s.length(), string.length())));
      }
      // System.out.println(mid+"---"+dictionary.idToString(mid,
      // TripleComponentRole.OBJECT).toString());
      String s = dictionary.idToString(mid, TripleComponentRole.OBJECT).toString();
      int c2 = comparator.compare(string, s.substring(0, Math.min(s.length(), string.length())));
      //            System.out.println("c "+c);
      //            System.out.println("c2 "+c2);
      if ((mid == 1 || c < 0) && c2 == 0) return mid;
      else if (c > 0) return first(dictionary, (mid + 1), high, string);
      else return first(dictionary, low, (mid - 1), string);
    }
    return -1;
  }

  // LAST occurrence of " in the dictionary between the indexes low and high, if not found return -1
  public static long last(Dictionary dictionary, long low, long high, long n, String string) {
    if (high >= low) {
      long mid = low + (high - low) / 2;
      //            System.out.println("low "+low);
      //            System.out.println("high "+high);
      //            System.out.println("mid "+mid);
      int c = -1;
      if (mid != n) {
        //                System.out.println("mid-1 "+(mid+11)+dictionary.idToString(mid + 1,
        // TripleComponentRole.OBJECT).toString());
        c =
            comparator.compare(
                string,
                dictionary
                    .idToString(mid + 1, TripleComponentRole.OBJECT)
                    .toString()
                    .subSequence(0, 1));
      }
      int c2 =
          comparator.compare(
              "\"",
              dictionary.idToString(mid, TripleComponentRole.OBJECT).toString().subSequence(0, 1));
      //            System.out.println("c"+c);
      //            System.out.println("c2 "+c2);
      if ((mid == n || c < 0) && c2 == 0) return mid;
      else if (c < 0) return last(dictionary, low, (mid - 1), n, string);
      else return last(dictionary, (mid + 1), high, n, string);
    }
    return -1;
  }
}
