/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/compact
 * /sequence/DeflateIntegerIterator.java $ Revision: $Rev: 191 $ Last modified:
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

package com.the_qa_company.qendpoint.core.compact.sequence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.zip.InflaterInputStream;

/**
 * @author mck
 */
public class DeflateIntegerIterator implements Iterator<Integer> {
	private static final Logger log = LoggerFactory.getLogger(DeflateIntegerIterator.class);

	protected final List<byte[]> buffer;
	protected DataInputStream stream;
	int current, next;

	public DeflateIntegerIterator(List<byte[]> buffer) {
		this.buffer = buffer;
	}

	public void reset(long list) {
		byte[] buf = buffer.get((int) list);
		try {
			stream = new DataInputStream(new InflaterInputStream(
//					new SnappyInputStream(
					new ByteArrayInputStream(buf)));

			try {
				current = stream.readInt();
				System.out.println("First: " + current);
				if (current == -1) {
					return;
				}
			} catch (IOException e) {
				current = -1;
			}

			try {
				int val = stream.readInt();
				System.out.println("Second: " + val);
				next = val != -1 ? current + val : -1;
			} catch (IOException e) {
				next = -1;
			}
		} catch (Exception e1) {
			log.error("Unexpected exception.", e1);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		return current != -1;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public Integer next() {
		int aux = current;
		current = next;

		try {
			int val = stream.readInt();
			next = val != -1 ? current + val : -1;
		} catch (IOException e) {
			next = -1;
		}

		return aux;
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
