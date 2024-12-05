/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/util/
 * BitUtil.java $ Revision: $Rev: 191 $ Last modified: $Date: 2013-03-03
 * 11:41:43 +0000 (dom, 03 mar 2013) $ Last modified by: $Author: mario.arias $
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; version 3.0 of the License. This library is distributed
 * in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * library; if not, write to the Free Software Foundation, Inc., 51 Franklin St,
 * Fifth Floor, Boston, MA 02110-1301 USA Contacting the authors: Mario Arias:
 * mario.arias@deri.org Javier D. Fernandez: jfergar@infor.uva.es Miguel A.
 * Martinez-Prieto: migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */
package com.the_qa_company.qendpoint.core.util;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class BitUtil {

	private BitUtil() {
	}

	/**
	 * Number of bits needed to store up to n
	 *
	 * @param n
	 */
	public static int log2(long n) {
		return (n != 0) ? (64 - Long.numberOfLeadingZeros(n)) : 0;
	}

	public static long maxVal(int numbits) {
		return numbits == Long.SIZE ? Long.MAX_VALUE : ~(~0L << numbits);
	}

	public static long readLowerBitsByteAligned(long numbits, InputStream in) throws IOException {
		int bitsRead = 0;
		long value = 0;
		while (bitsRead < numbits) {
			long readByte = in.read();
			if (readByte == -1)
				throw new EOFException();
			value |= readByte << bitsRead;
			bitsRead += 8;
		}
		return value;
	}

	public static void writeLowerBitsByteAligned(long value, long numbits, OutputStream out) throws IOException {
		while (numbits > 0) {
			out.write((int) (value & 0xFF));
			value >>>= 8;
			numbits -= 8;
		}
	}

	public static int select1(long value, int rank) {
		int bitpos = 0;
		while (rank > 0 && value != 0) {
			int trailingZeros = Long.numberOfTrailingZeros(value);
			bitpos += trailingZeros + 1;
			value >>>= trailingZeros + 1;
			rank--;
		}
		return bitpos;
	}

	public static int select0(long value, int rank) {
		int bitpos = 0;
		while (rank > 0) {
			rank -= (value ^ 1) & 1;
			bitpos++;
			value >>>= 1;
		}
		return bitpos;
	}
}
