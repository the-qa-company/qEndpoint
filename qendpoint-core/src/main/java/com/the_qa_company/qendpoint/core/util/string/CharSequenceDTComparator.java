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

import com.the_qa_company.qendpoint.core.util.LiteralsUtils;

import java.util.Comparator;

/**
 * @author mario.arias
 */
public final class CharSequenceDTComparator implements Comparator<CharSequence> {

	private static final Comparator<CharSequence> instance = new CharSequenceDTComparator();

	public static Comparator<CharSequence> getInstance() {
		return instance;
	}

	private final Comparator<CharSequence> base = CharSequenceComparator.getInstance();

	/*
	 * (non-Javadoc)
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	@Override
	public int compare(CharSequence s1, CharSequence s2) {
		if (s1 == s2) {
			return 0;
		}
		CharSequence type1 = LiteralsUtils.getType(s1);
		CharSequence type2 = LiteralsUtils.getType(s2);

		int x = base.compare(type1, type2);

		if (x != 0) {
			return x;
		} else { // data types are equal
			return base.compare(s1, s2);
		}
	}

}
