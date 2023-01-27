/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/util/
 * string/CharSequenceComparator.java $ Revision: $Rev: 200 $ Last modified:
 * $Date: 2013-04-17 23:36:44 +0100 (mi, 17 abr 2013) $ Last modified by:
 * $Author: mario.arias $ This library is free software; you can redistribute it
 * and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; version 3.0 of the License. This
 * library is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 * Contacting the authors: Mario Arias: mario.arias@deri.org Javier D.
 * Fernandez: jfergar@infor.uva.es Miguel A. Martinez-Prieto:
 * migumar2@infor.uva.es
 */

package com.the_qa_company.qendpoint.core.util.string;

import java.util.Comparator;

/**
 * @author mario.arias
 */
public final class CharSequenceComparator implements Comparator<CharSequence> {

	private static final Comparator<CharSequence> instance = new CharSequenceComparator();

	public static Comparator<CharSequence> getInstance() {
		return instance;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	@Override
	public int compare(CharSequence s1, CharSequence s2) {
		if (s1 == s2) {
			return 0;
		}

		s1 = DelayedString.unwrap(s1);
		s2 = DelayedString.unwrap(s2);

		if (s1 instanceof ByteString && s2 instanceof ByteString) {
			ByteString cs1 = (ByteString) s1;
			ByteString cs2 = (ByteString) s2;
			return cs1.compareTo(cs2);
		}

		if (s1 instanceof String && s2 instanceof String) {
			String rs1 = (String) s1;
			String rs2 = (String) s2;
			return rs1.compareTo(rs2);
		}

		// Slower but safe

		return s1.toString().compareTo(s2.toString());
//
//		int len1 = s1.length();
//        int len2 = s2.length();
//        int n = Math.min(len1, len2);
//
//        int k = 0;
//        while (k < n) {
//            char c1 = s1.charAt(k);
//            char c2 = s2.charAt(k);
//            if (c1 != c2) {
//                return c2 - c1;
//            }
//            k++;
//        }
//        return len2 - len1;
	}

}
