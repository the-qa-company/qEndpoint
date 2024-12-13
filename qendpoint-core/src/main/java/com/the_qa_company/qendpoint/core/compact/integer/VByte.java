/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/compact
 * /integer/VByte.java $ Revision: $Rev: 191 $ Last modified: $Date: 2013-03-03
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

package com.the_qa_company.qendpoint.core.compact.integer;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.Mutable;
import com.the_qa_company.qendpoint.core.util.io.BigByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.BigMappedByteBuffer;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.ReplazableString;

/**
 * Typical implementation of Variable-Byte encoding for integers. <a href=
 * "http://nlp.stanford.edu/IR-book/html/htmledition/variable-byte-codes-1.html">variable-byte-codes</a>
 * The first bit of each byte specifies whether there are more bytes available.
 * Numbers from 0 to 126 are encoded using just one byte. Numbers from 127 to
 * 16383 are encoded using two bytes. Numbers from 16384 to 2097151 are encoded
 * using three bytes.
 *
 * @author mario.arias
 */
public class VByte {

	private VByte() {
	}

	/**
	 * encode a Variable-Byte adding a bit for the sign, should be decoded with
	 * {@link #decodeSigned(InputStream)}
	 *
	 * @param out   out stream
	 * @param value value to encode
	 * @throws IOException write exception
	 */
	public static void encodeSigned(OutputStream out, long value) throws IOException {
		if (value < 0) {
			// set the 1st bit to 1
			encode(out, ~(value << 1));
		} else {
			encode(out, value << 1);
		}
	}

	/**
	 * decode a signed Variable-Byte, should be encoded with
	 * {@link #encodeSigned(OutputStream, long)}
	 *
	 * @param in in stream
	 * @return decoded value
	 * @throws IOException write exception
	 */
	public static long decodeSigned(InputStream in) throws IOException {
		long decode = decode(in);
		if ((decode & 1) == 0) {
			// +
			return decode >>> 1;
		} else {
			// -
			return ~(decode >>> 1);
		}
	}

	public static void encode(OutputStream out, long value) throws IOException {
		if (value < 0) {
			throw new IllegalArgumentException("Only can encode VByte of positive values");
		}
		while (value > 127) {
			out.write((int) (value & 127));
			value >>>= 7;
		}
		out.write((int) (value | 0x80));
	}

	public static void encodeStr(ReplazableString out, long value) {
		if (value <= 0) {
			throw new IllegalArgumentException("Only can encode VByte of positive values to string");
		}
		while (value > 0x7F) {
			out.append((byte) ((value & 0x7F) | 0x80));
			value >>>= 7;
		}
		out.append((byte) (value));
	}

	public static long decode(InputStream in) throws IOException {
		long out = 0;
		int shift = 0;
		long readbyte = in.read();
		if (readbyte == -1) {
			throw new EOFException();
		}

		while ((readbyte & 0x80) == 0) {
			if (shift >= 50) { // We read more bytes than required to load the
								// max long
				throw new IllegalArgumentException("Read more bytes than required to load the max long");
			}

			out |= (readbyte & 127) << shift;

			readbyte = in.read();
			if (readbyte == -1)
				throw new EOFException();

			shift += 7;
		}
		out |= (readbyte & 127) << shift;
		return out;
	}

	public static long decode(ByteBuffer in) throws IOException {
		long out = 0;
		int shift = 0;
		if (!in.hasRemaining())
			throw new EOFException();
		byte readbyte = in.get();

		while ((readbyte & 0x80) == 0) {
			if (shift >= 50) { // We read more bytes than required to load the
								// max long
				throw new IllegalArgumentException("Read more bytes than required to load the max long");
			}

			out |= (readbyte & 127L) << shift;

			if (!in.hasRemaining())
				throw new EOFException();
			readbyte = in.get();

			shift += 7;
		}
		out |= (readbyte & 127L) << shift;
		return out;
	}

	public static long decode(BigMappedByteBuffer in) throws IOException {
		long out = 0;
		int shift = 0;
		if (!in.hasRemaining())
			throw new EOFException();
		byte readbyte = in.get();

		while ((readbyte & 0x80) == 0) {
			if (shift >= 50) { // We read more bytes than required to load the
								// max long
				throw new IllegalArgumentException("Read more bytes than required to load the max long");
			}

			out |= (readbyte & 127L) << shift;

			if (!in.hasRemaining())
				throw new EOFException();
			readbyte = in.get();

			shift += 7;
		}
		out |= (readbyte & 127L) << shift;
		return out;
	}

	public static int encode(byte[] data, int offset, int value) {
		if (value < 0) {
			throw new IllegalArgumentException("Only can encode VByte of positive values");
		}
		int i = 0;
		while (value > 127) {
			data[offset + i] = (byte) (value & 127);
			i++;
			value >>>= 7;
		}
		data[offset + i] = (byte) (value | 0x80);
		i++;

		return i;
	}

	public static int decode(byte[] data, int offset, Mutable<Long> value) {
		long out = 0;
		int i = 0;
		int shift = 0;
		while ((0x80 & data[offset + i]) == 0) {
			assert shift < 50 : "Read more bytes than required to load the max long";
			out |= (data[offset + i] & 127L) << shift;
			i++;
			shift += 7;
		}
		out |= (data[offset + i] & 127L) << shift;
		i++;
		value.setValue(out);
		return i;
	}

	public static int decodeStr(ByteString data, int offset, Mutable<Long> value) {
		long out = 0;
		int i = 0;
		int shift = 0;
		while ((0x80 & data.charAt(offset + i)) != 0) {
			assert shift < 50 : "Read more bytes than required to load the max long";
			out |= (data.charAt(offset + i) & 0x7FL) << shift;
			i++;
			shift += 7;
		}
		out |= (data.charAt(offset + i) & 0x7FL) << shift;
		i++;
		value.setValue(out);
		return i;
	}

	public static int decode(BigByteBuffer data, long offset, Mutable<Long> value) {
		long out = 0;
		int i = 0;
		int shift = 0;
		while ((0x80 & data.get(offset + i)) == 0) {
			assert shift < 50 : "Read more bytes than required to load the max long";
			out |= (data.get(offset + i) & 127L) << shift;
			i++;
			shift += 7;
		}
		out |= (data.get(offset + i) & 127L) << shift;
		i++;
		value.setValue(out);
		return i;
	}

	public static void show(byte[] data, int len) {
		for (int i = 0; i < len; i++) {
			System.out.print(Long.toHexString(data[i] & 0xFF) + " ");
		}
	}

	public static int sizeOf(long number) {
		return (BitUtil.log2(number) - 1) / 7 + 1;
	}

	public static int sizeOfSigned(long number) {
		return (BitUtil.log2(number << 1) - 1) / 7 + 1;
	}
}
