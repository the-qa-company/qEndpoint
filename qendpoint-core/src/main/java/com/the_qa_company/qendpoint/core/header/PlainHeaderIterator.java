/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/header/
 * PlainHeaderIterator.java $ Revision: $Rev: 191 $ Last modified: $Date:
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

package com.the_qa_company.qendpoint.core.header;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.enums.ResultEstimationType;

/**
 * @author mario.arias
 */
public class PlainHeaderIterator implements IteratorTripleString {
	private final PlainHeader header;
	private int pos;
	private TripleString nextTriple;
	private final TripleString pattern;
	private TripleString returnTriple;
	private boolean hasMoreTriples;

	/**
	 *
	 */
	public PlainHeaderIterator(PlainHeader header, TripleString pattern) {
		this.header = header;
		this.pattern = pattern;
		this.pos = 0;

		doFetch();
	}

	private void doFetch() {
		do {
			getNextTriple();
		} while (hasMoreTriples && (!nextTriple.match(pattern)));
	}

	private void getNextTriple() {
		if (pos < header.triples.size()) {
			nextTriple = header.triples.get(pos);
		}

		pos++;

		hasMoreTriples = pos <= header.triples.size();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleString#hasNext()
	 */
	@Override
	public boolean hasNext() {
		return hasMoreTriples;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleString#next()
	 */
	@Override
	public TripleString next() {
		returnTriple = nextTriple;
		doFetch();
		return returnTriple;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.iterator.IteratorTripleString#goToStart()
	 */
	@Override
	public void goToStart() {
		pos = 0;
		doFetch();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public long estimatedNumResults() {
		return header.triples.size();
	}

	@Override
	public ResultEstimationType numResultEstimation() {
		return ResultEstimationType.UP_TO;
	}

	@Override
	public long getLastTriplePosition() {
		throw new UnsupportedOperationException();
	}

	@Override
	public TripleComponentOrder getOrder() {
		return TripleComponentOrder.Unknown;
	}

	@Override
	public boolean isLastTriplePositionBoundToOrder() {
		return false;
	}
}
