/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/util/
 * string/ReplazableString.java $ Revision: $Rev: 191 $ Last modified: $Date:
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
 * migumar2@infor.uva.es
 */

package com.the_qa_company.qendpoint.core.util.string;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.util.io.BigByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.BigMappedByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

/**
 * CharSequence implementation suitable for appending or replacing the suffix of
 * the string. It grows as necessary but it never returns that size back.
 *
 * @author mario.arias
 */
public final class ReplazableString implements CharSequence, ByteString {

	byte[] buffer;
	int used;

	public ReplazableString() {
		this(128);
	}

	public ReplazableString(int initialCapacity) {
		buffer = new byte[initialCapacity];
		used = 0;
	}

	public ReplazableString(byte[] buffer) {
		this.buffer = buffer;
		this.used = buffer.length;
	}

	@Override
	public byte[] getBuffer() {
		return buffer;
	}

	public void clear() {
		used = 0;
	}

	private void ensureSize(int size) {
		if (size > buffer.length) {
			buffer = Arrays.copyOf(buffer, Math.max(size, buffer.length * 2));
		}
	}

	public void append(byte c) {
		ensureSize(this.used + 1);
		buffer[this.used++] = c;
	}
	public void append(byte[] data) {
		this.append(data, 0, data.length);
	}

	public void append(byte[] data, int offset, int len) {
		this.replace(used, data, offset, len);
	}

	public void append(BigByteBuffer data, long offset, int len) {
		this.replace(used, data, offset, len);
	}

	public void append(CharSequence other) {
		ensureSize(this.used + other.length());
		for (int i = 0; i < other.length(); i++) {
			buffer[this.used + i] = (byte) other.charAt(i);
		}
		used += other.length();
	}

	public void appendNoCompact(CharSequence other) {
		other = DelayedString.unwrap(other);

		if (other instanceof ByteString) {
			this.appendNoCompact((ByteString) other);
		} else {
			this.append(other.toString().getBytes(ByteStringUtil.STRING_ENCODING));
		}
	}

	public void appendNoCompact(ByteString other) {
		this.append(other.getBuffer(), 0, other.length());
	}

	public void appendNoCompact(CharSequence other, int offset, int length) {
		other = DelayedString.unwrap(other);

		if (other instanceof ByteString) {
			this.append(((ByteString) other).getBuffer(), offset, length);
		} else {
			this.append(other.toString().substring(offset, offset + length).getBytes(ByteStringUtil.STRING_ENCODING));
		}
	}

	public void replace(ByteString other) {
		ensureSize(other.length());
		System.arraycopy(other.getBuffer(), 0, buffer, 0, other.length());
		used = other.length();
	}

	public void replace(CharSequence other) {
		if (other instanceof ByteString) {
			replace((ByteString) other);
		} else {
			used = 0;
			byte[] bytes = other.toString().getBytes(StandardCharsets.UTF_8);
			replace(0, bytes, 0, bytes.length);
		}
	}

	public void replace(int pos, byte[] data, int offset, int len) {
		ensureSize(pos + len);
		System.arraycopy(data, offset, buffer, pos, len);
		used = pos + len;
	}

	public void replace(int pos, BigByteBuffer data, long offset, int len) {
		ensureSize(pos + len);
		data.get(buffer, offset, pos, len);
		used = pos + len;
	}

	public void replace(InputStream in, int pos, int len) throws IOException {
		byte[] buffer = IOUtil.readBuffer(in, len, null);
		replace(pos, buffer, 0, len);
	}

	public void replace(ByteBuffer in, int pos, int len) throws IOException {
		ensureSize(pos + len);
		in.get(buffer, pos, len);
		used = pos + len;
	}

	public void replace(BigMappedByteBuffer in, int pos, int len) throws IOException {
		ensureSize(pos + len);
		in.get(buffer, pos, len);
		used = pos + len;
	}

	public void replace2(InputStream in, int pos) throws IOException {
		used = pos;

		while (true) {
			int value = in.read();
			if (value == -1) {
				throw new IllegalArgumentException(
						"Was reading a string but stream ended before finding the null terminator");
			}
			if (value == 0) {
				break;
			}
			if (used >= buffer.length) {
				buffer = Arrays.copyOf(buffer, buffer.length * 2);
			}
			buffer[used++] = (byte) (value & 0xFF);
		}
	}

	private static final int READ_AHEAD = 1024;

	public void replace(InputStream in, int pos) throws IOException {

		if (!in.markSupported()) {
			replace2(in, pos);
			return;
		}
		used = pos;

		while (true) {
			if (used + READ_AHEAD > buffer.length) {
				buffer = Arrays.copyOf(buffer, Math.max(buffer.length * 2, used + READ_AHEAD));
			}
			in.mark(READ_AHEAD);
			int numread = in.read(buffer, used, READ_AHEAD);
			if (numread == -1) {
				throw new IllegalArgumentException(
						"Was reading a string but stream ended before finding the null terminator");
			}

			int i = 0;
			while (i < numread) {
//				System.out.println("Char: "+buffer[used+i]+"/"+(char)buffer[used+i]);
				if (buffer[used + i] == 0) {
					in.reset();
					in.skip(i + 1);
					used += i;
					return;
				}
				i++;
			}
			used += numread;
		}
	}

	public void replace(ByteBuffer in, int pos) throws IOException {
		used = pos;

		int n = in.capacity() - in.position();
		while (n-- != 0) {
			byte value = in.get();
			if (value == 0) {
				return;
			}
			if (used >= buffer.length) {
				buffer = Arrays.copyOf(buffer, buffer.length * 2);
			}
			buffer[used++] = value;
		}
		throw new IllegalArgumentException("Was reading a string but stream ended before finding the null terminator");
	}

	public void replace(BigMappedByteBuffer in, int pos) throws IOException {
		used = pos;

		long n = in.capacity() - in.position();
		while (n-- != 0) {
			byte value = in.get();
			if (value == 0) {
				return;
			}
			if (used >= buffer.length) {
				buffer = Arrays.copyOf(buffer, buffer.length * 2);
			}
			buffer[used++] = value;
		}
		throw new IllegalArgumentException("Was reading a string but stream ended before finding the null terminator");
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.CharSequence#charAt(int)
	 */
	@Override
	public char charAt(int index) {
		if (index >= used) {
			throw new StringIndexOutOfBoundsException("Invalid index " + index + " length " + length());
		}
		return (char) (buffer[index] & 0xFF);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.CharSequence#length()
	 */
	@Override
	public int length() {
		return used;
	}

	@Override
	public int hashCode() {
		// FNV Hash function: http://isthe.com/chongo/tech/comp/fnv/
		int hash = (int) 2166136261L;
		int i = used;

		while (i-- != 0) {
			hash = (hash * 16777619) ^ buffer[i];
		}

		return hash;
	}

	@Override
	public boolean equals(Object o) {
		if (o == null) {
			return false;
		}
		if (this == o) {
			return true;
		}
		if (o instanceof CompactString) {
			CompactString cmp = (CompactString) o;
			if (buffer.length != cmp.data.length) {
				return false;
			}

			// Byte by byte comparison
			int i = buffer.length;
			while (i-- != 0) {
				if (buffer[i] != cmp.data[i]) {
					return false;
				}
			}
			return true;
		} else if (o instanceof ReplazableString cmp) {
			if (this.used != cmp.used) {
				return false;
			}

			// Byte by byte comparison
			int i = this.used;
			while (i-- != 0) {
				if (buffer[i] != cmp.buffer[i]) {
					return false;
				}
			}
			return true;
		} else if (o instanceof CharSequence other) {
			return length() == other.length() && CharSequenceComparator.getInstance().compare(this, other) == 0;
		}
		throw new NotImplementedException();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.CharSequence#subSequence(int, int)
	 */
	@Override
	public ByteString subSequence(int start, int end) {
		if (start < 0 || end > (this.length()) || (end - start) < 0) {
			throw new IllegalArgumentException(
					"Illegal range " + start + "-" + end + " for sequence of length " + length());
		}
		byte[] newdata = new byte[end - start];
		System.arraycopy(buffer, start, newdata, 0, end - start);
		return new ReplazableString(newdata);
	}

	@Override
	public String toString() {
		return new String(buffer, 0, used, ByteStringUtil.STRING_ENCODING);
	}

	public CharSequence getDelayed() {
		return new DelayedString(this);
	}
}
