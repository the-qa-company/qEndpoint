/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/triples
 * /impl/TripleOrderConvert.java $ Revision: $Rev: 191 $ Last modified: $Date:
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

package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.triples.TripleID;

/**
 * @author mario.arias
 */
public class TripleOrderConvert {
	/*
	 * This table reflects the swaps needed to convert from a SRC ComponentOrder
	 * to a DST ComponentOrder. There are three swaps possible: swap1 (x and y)
	 * swap2 (y and z) swap3 (x and z) The order of swaps is important, there
	 * are five different: xy 12 xz 13 yz 23 xyz1 12 13 xyz2 12 23 SRC v SPO SOP
	 * PSO POS OSP OPS <DEST SPO yz xy xyz2 xyz1 xz SOP yz xyz1 xz xy xyz2 PSO
	 * xy xyz2 yz xz xyz1 POS xyz1 xz yz xyz2 OSP xyz2 xy xz xyz1 yz OPS xz xyz1
	 * xyz2 xy yz
	 */

	// swap 1-2
	// xy xyz1 xyz2
	private static final boolean[][] swap1tab = new boolean[][] { { false, false, true, true, true, false },
			{ false, false, true, false, true, true }, { true, true, false, false, false, true },
			{ true, false, false, false, true, false }, { true, true, false, true, false, false },
			{ false, true, true, true, false, false } };

	// swap 1-3
	// xz xyz1
	private static final boolean[][] swap2tab = new boolean[][] { { false, false, false, false, true, true },
			{ false, false, true, true, false, false }, { false, false, false, false, true, true },
			{ true, true, false, false, false, false }, { false, false, true, true, false, false },
			{ true, true, false, false, false, false } };

	// swap 2-3
	// yz xyz2
	private static final boolean[][] swap3tab = new boolean[][] { { false, true, false, true, false, false },
			{ true, false, false, false, false, true }, { false, true, false, true, false, false },
			{ false, false, true, false, true, false }, { true, false, false, false, false, true },
			{ false, false, true, false, true, false } };

	private TripleOrderConvert() {
	}

	public static void swapComponentOrder(TripleID triple, TripleComponentOrder from, TripleComponentOrder to) {
		if (from == to) {
			return;
		}
		if (from == TripleComponentOrder.Unknown || to == TripleComponentOrder.Unknown) {
			throw new IllegalArgumentException("Cannot swap Unknown Orders");
		}

		swap1(triple, from, to);
		swap2(triple, from, to);
		swap3(triple, from, to);
	}

	private static void swap3(TripleID triple, TripleComponentOrder from, TripleComponentOrder to) {
		if (swap3tab[from.ordinal() - 1][to.ordinal() - 1]) {
			long tmp = triple.getPredicate();
			triple.setPredicate(triple.getObject());
			triple.setObject(tmp);
		}
	}

	private static void swap2(TripleID triple, TripleComponentOrder from, TripleComponentOrder to) {
		if (swap2tab[from.ordinal() - 1][to.ordinal() - 1]) {
			long tmp = triple.getSubject();
			triple.setSubject(triple.getObject());
			triple.setObject(tmp);
		}
	}

	private static void swap1(TripleID triple, TripleComponentOrder from, TripleComponentOrder to) {
		if (swap1tab[from.ordinal() - 1][to.ordinal() - 1]) {
			long tmp = triple.getSubject();
			triple.setSubject(triple.getPredicate());
			triple.setPredicate(tmp);
		}
	}
}
