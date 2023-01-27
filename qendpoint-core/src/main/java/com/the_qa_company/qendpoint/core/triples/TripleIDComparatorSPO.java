/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/triples
 * /TripleIDComparator.java $ Revision: $Rev: 191 $ Last modified: $Date:
 * 2013-03-03 11:41:43 +0000 (dom, 03 mar 2013) $ Last modified by: $Author:
 * mario.arias $ This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; version 3.0 of the License. This
 * library is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 * Contacting the authors: Mario Arias: mario.arias@deri.org Javier D.
 * Fernandez: jfergar@infor.uva.es Miguel A. Martinez-Prieto:
 * migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.triples;

import java.util.Comparator;

import com.the_qa_company.qendpoint.core.util.LongCompare;

/**
 * Comparator between triples, based on the TripleComponentOrder
 */
public final class TripleIDComparatorSPO implements Comparator<TripleID> {

	// Singleton
	private static TripleIDComparatorSPO instance;

	public static TripleIDComparatorSPO getInstance() {
		if (instance == null) {
			instance = new TripleIDComparatorSPO();
		}
		return instance;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	@Override
	public int compare(TripleID o1, TripleID o2) {

		int result = LongCompare.compare(o1.getSubject(), o2.getSubject());

		if (result == 0) {
			result = LongCompare.compare(o1.getPredicate(), o2.getPredicate());
			if (result == 0) {
				// The third component is different?
				return LongCompare.compare(o1.getObject(), o2.getObject());
			} else {
				// the second component is different
				return result;
			}
		} else {
			// the first component is different
			return result;
		}
	}

}
