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

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.util.LongCompare;

/**
 * Comparator between triples, based on the TripleComponentOrder
 */
public class TripleIDComparator implements Comparator<TripleID>, Serializable {
	private static final long serialVersionUID = -8322949509663015732L;

	/** Determines the order of comparison */
	private final TripleComponentOrder order;

	public static Comparator<TripleID> getComparator(TripleComponentOrder order) {
		if (order == TripleComponentOrder.SPO) {
			return TripleIDComparatorSPO.getInstance();
		}
		return new TripleIDComparator(order);
	}

	/**
	 * Basic constructor
	 *
	 * @param order The order to compare with
	 */
	public TripleIDComparator(TripleComponentOrder order) {
		super();
		this.order = order;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	@Override
	public int compare(TripleID o1, TripleID o2) {
		/*
		 * Returns a negative integer, zero, or a positive integer as the first
		 * argument is less than, equal to, or greater than the second.
		 */
		/*
		 * Components of the triple. Meaning will be given based on the order
		 * variable, see below
		 */
		long x1 = 0, y1 = 0, z1 = 0, x2 = 0, y2 = 0, z2 = 0;

		switch (this.order) {
		case SPO:
			// Subjects
			x1 = o1.getSubject();
			x2 = o2.getSubject();
			// Predicates
			y1 = o1.getPredicate();
			y2 = o2.getPredicate();
			// Objects
			z1 = o1.getObject();
			z2 = o2.getObject();
			break;
		case SOP:
			// Subjects
			x1 = o1.getSubject();
			x2 = o2.getSubject();
			// Objects
			y1 = o1.getObject();
			y2 = o2.getObject();
			// Predicates
			z1 = o1.getPredicate();
			z2 = o2.getPredicate();
			break;
		case PSO:
			// Predicates
			x1 = o1.getPredicate();
			x2 = o2.getPredicate();
			// Subjects
			y1 = o1.getSubject();
			y2 = o2.getSubject();
			// Objects
			z1 = o1.getObject();
			z2 = o2.getObject();
			break;
		case POS:
			// Predicates
			x1 = o1.getPredicate();
			x2 = o2.getPredicate();
			// Objects
			y1 = o1.getObject();
			y2 = o2.getObject();
			// Subjects
			z1 = o1.getSubject();
			z2 = o2.getSubject();
			break;
		case OSP:
			// Objects
			x1 = o1.getObject();
			x2 = o2.getObject();
			// Subjects
			y1 = o1.getSubject();
			y2 = o2.getSubject();
			// Predicates
			z1 = o1.getPredicate();
			z2 = o2.getPredicate();
			break;
		case OPS:
			// Objects
			x1 = o1.getObject();
			x2 = o2.getObject();
			// Predicates
			y1 = o1.getPredicate();
			y2 = o2.getPredicate();
			// Subjects
			z1 = o1.getSubject();
			z2 = o2.getSubject();
			break;
		case Unknown:
			throw new IllegalArgumentException("Cannot change order of triple if order is unknown.");
		default:
			break;
		}

		int result = LongCompare.compare(x1, x2);

		if (result == 0) {
			result = LongCompare.compare(y1, y2);
			if (result == 0) {
				// The third component is different?
				result = LongCompare.compare(z1, z2);
				if (result == 0) {
					return LongCompare.compare(o1.getGraph(), o2.getGraph());
				}
				return result;
			} else {
				// the second component is different
				return result;
			}
		} else {
			// the first component is different
			return result;
		}
	}

	/**
	 * method for serialization of an instance of this class as specified by
	 * Serializable
	 */
	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		out.defaultWriteObject();
	}

	/**
	 * method for deserialization of an instance of this class as specified by
	 * Serializable
	 */
	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
	}
}
