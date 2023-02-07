/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/
 * iterator/RoleIteratorTripleID.java $ Revision: $Rev: 191 $ Last modified:
 * $Date: 2013-03-03 11:41:43 +0000 (dom, 03 mar 2013) $ Last modified by:
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
 * migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.iterator;

import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.triples.TripleID;

import java.util.Iterator;

/**
 * Iterates over one of the components of a triple (E.g. all subjects,
 * predicates or objects) NOTE: It does not discard repetitions
 *
 * @author mario.arias
 */
public class RoleIteratorTripleID implements Iterator<Long> {

	final TripleComponentRole position;
	final Iterator<TripleID> iterator;

	/**
	 * @param iterator
	 * @param position
	 */
	public RoleIteratorTripleID(Iterator<TripleID> iterator, TripleComponentRole position) {
		this.position = position;
		this.iterator = iterator;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public Long next() {
		switch (position) {
		case SUBJECT:
			return iterator.next().getSubject();
		case PREDICATE:
			return iterator.next().getPredicate();
		case OBJECT:
			return iterator.next().getObject();
		}
		// Never reached
		return 0L;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
