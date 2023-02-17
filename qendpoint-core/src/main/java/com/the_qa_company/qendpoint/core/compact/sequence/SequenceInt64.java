/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/compact
 * /sequence/SequenceInt64.java $ Revision: $Rev: 191 $ Last modified: $Date:
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

package com.the_qa_company.qendpoint.core.compact.sequence;

import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Stream of Integer objects
 */
public class SequenceInt64 implements DynamicSequence {

	/** The array that holds the objects **/
	long[] data;
	long numelements;

	/**
	 * Basic constructor
	 */
	public SequenceInt64() {
		this(10);
	}

	public SequenceInt64(long capacity) {
		assert capacity >= 0 && capacity <= Integer.MAX_VALUE;

		data = new long[(int) capacity];
		numelements = 0;
	}

	@Override
	public void resize(long numentries) {
		if (numentries > (long) Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Can't resize to size bigger than int value");
		}
		resizeArray((int) numentries);
		this.numelements = numentries;
	}

	@Override
	public void clear() {
		Arrays.fill(data, 0);
	}

	private void resizeArray(int size) {
		long[] newData = new long[size];
		System.arraycopy(data, 0, newData, 0, Math.min(newData.length, data.length));
		data = newData;
	}

	@Override
	public void trimToSize() {
		resizeArray((int) numelements);
	}

	@Override
	public void aggressiveTrimToSize() {
		trimToSize();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.array.Stream#get(int)
	 */
	@Override
	public long get(long position) {
		assert position >= 0 && position <= Integer.MAX_VALUE;

		return data[(int) position];
	}

	@Override
	public void set(long position, long value) {
		assert position >= 0 && position <= Integer.MAX_VALUE;
		assert value >= 0;

		data[(int) position] = value;
		numelements = (int) Math.max(numelements, position + 1);
	}

	@Override
	public int sizeOf() {
		return 64;
	}

	@Override
	public void append(long value) {
		assert value >= 0;
		assert numelements < Long.MAX_VALUE;

		long neededSize = numelements + 1L;
		if (neededSize > Integer.MAX_VALUE - 5) {
			throw new IllegalArgumentException(
					"Needed size exceeds the maximum size of this data structure " + neededSize);
		}
		if (data.length < neededSize) {
			resizeArray((int) Math.min(Integer.MAX_VALUE - 5L, data.length * 2L));
		}
		data[(int) numelements++] = value;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.array.Stream#getNumberOfElements()
	 */
	@Override
	public long getNumberOfElements() {
		return numelements;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.array.Stream#save(java.io.OutputStream)
	 */
	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {

		output.write(SequenceFactory.TYPE_SEQ64);

		IOUtil.writeLong(output, numelements);

		for (int i = 0; i < numelements; i++) {
			IOUtil.writeLong(output, data[i]);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.array.Stream#load(java.io.InputStream)
	 */
	@Override
	public void load(InputStream input, ProgressListener listener) throws IOException {

		int type = input.read();
		if (type != SequenceFactory.TYPE_SEQ64) {
			throw new IllegalFormatException("Trying to read INT64 but data is not INT64");
		}
		long size = IOUtil.readLong(input);

		numelements = 0;
		data = new long[(int) size];

		for (long i = 0; i < size; i++) {
			append(IOUtil.readLong(input));
		}
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.array.Stream#add(int)
	 */
	@Override
	public void add(Iterator<Long> iterator) {
		while (iterator.hasNext()) {
			long value = iterator.next();
			append(value);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		throw new NotImplementedException();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.triples.array.Stream#size()
	 */
	@Override
	public long size() {
		return 4 * numelements;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.compact.array.Stream#getType()
	 */
	@Override
	public String getType() {
		return HDTVocabulary.SEQ_TYPE_INT64;
	}

	@Override
	public void close() throws IOException {
		data = null;
	}
}
