package com.the_qa_company.qendpoint.utils;

import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.util.string.CharSequenceComparator;

/**
 * Small utility class to find literals in the dictionary using binary search
 */
public class BinarySearch {
	private static final CharSequenceComparator COMPARATOR = new CharSequenceComparator();

	/**
	 * FIRST occurrence of " in the dictionary between the indexes low and high
	 *
	 * @param dictionary the dictionary to search
	 * @param low        the lowest index
	 * @param high       the highest index
	 * @param string     the string to search
	 * @param role       role to search
	 * @return the index, or -1 for not found
	 */
	public static long first(Dictionary dictionary, long low, long high, String string, TripleComponentRole role) {
		if (high >= low) {
			long mid = (high + low) / 2;
			int c = -1;
			if (mid != 1) {
				String s = dictionary.idToString(mid - 1, role).toString();
				c = COMPARATOR.compare(string, s.substring(0, Math.min(s.length(), string.length())));
			}
			String s = dictionary.idToString(mid, role).toString();
			int c2 = COMPARATOR.compare(string, s.substring(0, Math.min(s.length(), string.length())));
			if ((mid == 1 || c != 0) && c2 == 0)
				return mid;
			else if (c > 0)
				return first(dictionary, (mid + 1), high, string, role);
			else
				return first(dictionary, low, (mid - 1), string, role);
		}
		return -1;
	}

	/**
	 * LAST occurrence of " in the dictionary between the indexes low and high
	 *
	 * @param dictionary the dictionary to search
	 * @param low        the lowest index
	 * @param high       the highest index
	 * @param n          n
	 * @param string     the string to search
	 * @param role       role to search
	 * @return the index, or -1 for not found
	 */
	public static long last(Dictionary dictionary, long low, long high, long n, String string,
			TripleComponentRole role) {
		if (high >= low) {
			long mid = (high + low) / 2;
			int c = -1;
			if (mid != n) {
				String s = dictionary.idToString(mid + 1, role).toString();
				c = COMPARATOR.compare(string, s.isEmpty() ? "" : s.subSequence(0, 1));
			}
			String s = dictionary.idToString(mid, role).toString();
			int c2 = COMPARATOR.compare(string, s.isEmpty() ? "": s.subSequence(0, 1));
			// System.out.println("c"+c);
			// System.out.println("c2 "+c2);
			if ((mid == n || c != 0) && c2 == 0)
				return mid;
			else if (c < 0)
				return last(dictionary, low, (mid - 1), n, string, role);
			else
				return last(dictionary, (mid + 1), high, n, string, role);
		}
		return -1;
	}
}
