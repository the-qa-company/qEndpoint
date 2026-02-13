/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/util/
 * string/ByteStringUtil.java $ Revision: $Rev: 199 $ Last modified: $Date:
 * 2013-04-17 23:35:53 +0100 (mi, 17 abr 2013) $ Last modified by: $Author:
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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.util.io.BigByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.BigMappedByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author mario.arias
 */
public class ByteStringUtil {

	private ByteStringUtil() {
	}

	/**
	 * For use in the project when using String.getBytes() and making Strings
	 * from byte[]
	 */
	public static final Charset STRING_ENCODING = UTF_8;

	public static String asString(byte[] buff, int offset) {
		int len = strlen(buff, offset);
		return new String(buff, offset, len, STRING_ENCODING);
	}

	public static String asString(ByteBuffer buff, int offset) {
		int len = strlen(buff, offset);
		byte[] arr = new byte[len];

		int i = 0;
		while (i < len) {
			arr[i] = buff.get(offset + i);
			i++;
		}
		return new String(arr, STRING_ENCODING);
	}

	/**
	 * convert this char sequence to a byte string (if required)
	 *
	 * @param sec the char sequence
	 * @return byte string
	 */
	public static ByteString asByteString(CharSequence sec) {
		sec = DelayedString.unwrap(sec);

		if (sec == null) {
			return null;
		}

		if (sec.isEmpty()) {
			return ByteString.empty();
		}

		if (sec instanceof ByteString bs) {
			return bs;
		}
		// clone into sec
		return new CompactString(sec);
	}

	public static int strlen(byte[] buff, int off) {
		int len = buff.length;
		int pos = off;
		while (pos < len && buff[pos] != 0) {
			pos++;
		}
		return pos - off;
	}

	public static long strlen(BigByteBuffer buff, long off) {
		long len = buff.size();
		long pos = off;
		while (pos < len && buff.get(pos) != 0) {
			pos++;
		}
		return pos - off;
	}

	public static int strlen(ByteBuffer buf, int base) {
		int len = 0;
		int n = buf.capacity() - base;
		while (len < n) {
			if (buf.get(base + len) == 0) {
				return len;
			}
			len++;
		}
		throw new IllegalArgumentException("Buffer not Null-Terminated");
	}

	public static int longestCommonPrefix(CharSequence str1, CharSequence str2) {
		return longestCommonPrefix(str1, str2, 0);
	}

	public static int longestCommonPrefix(CharSequence str1, CharSequence str2, int from) {
		// Match the style used elsewhere in this util: get rid of lazy wrappers
		// up front.
		str1 = DelayedString.unwrap(str1);
		str2 = DelayedString.unwrap(str2);

		// Preserve old behavior for negative "from":
		// the old loop would hit charAt(from) and throw from there.
		if (from < 0) {
			str1.charAt(from);
		}

		// Same reference => everything from 'from' matches.
		if (str1 == str2) {
			final int len = str1.length();
			return from >= len ? 0 : (len - from);
		}

		final int len = Math.min(str1.length(), str2.length());
		if (from >= len) {
			return 0;
		}

		final byte[] a = byteArrayOrNull(str1);
		final byte[] b = byteArrayOrNull(str2);
		if (a != null && b != null) {
			return lcpMismatch(a, b, from, len);
		}

		if (str1 instanceof CharBuffer cb1 && str2 instanceof CharBuffer cb2 && cb1.hasArray() && cb2.hasArray()) {
			return charBufMismatch(from, cb1, cb2, len);
		}

		return fallback(str1, str2, from, len);
	}

	private static int fallback(CharSequence str1, CharSequence str2, int from, int len) {
		int i = from;
		int remaining = len - from;

		while (remaining >= 8) {
			if (str1.charAt(i) != str2.charAt(i))
				return i - from;
			if (str1.charAt(i + 1) != str2.charAt(i + 1))
				return i + 1 - from;
			if (str1.charAt(i + 2) != str2.charAt(i + 2))
				return i + 2 - from;
			if (str1.charAt(i + 3) != str2.charAt(i + 3))
				return i + 3 - from;
			if (str1.charAt(i + 4) != str2.charAt(i + 4))
				return i + 4 - from;
			if (str1.charAt(i + 5) != str2.charAt(i + 5))
				return i + 5 - from;
			if (str1.charAt(i + 6) != str2.charAt(i + 6))
				return i + 6 - from;
			if (str1.charAt(i + 7) != str2.charAt(i + 7))
				return i + 7 - from;

			i += 8;
			remaining -= 8;
		}

		while (remaining-- > 0 && str1.charAt(i) == str2.charAt(i)) {
			i++;
		}
		return i - from;
	}

	private static int charBufMismatch(int from, CharBuffer cb1, CharBuffer cb2, int len) {
		final int base1 = cb1.arrayOffset() + cb1.position();
		final int base2 = cb2.arrayOffset() + cb2.position();

		final int mismatch = Arrays.mismatch(cb1.array(), base1 + from, base1 + len, cb2.array(), base2 + from,
				base2 + len);
		return mismatch < 0 ? (len - from) : mismatch;
	}

	private static byte[] byteArrayOrNull(CharSequence s) {
		// ReplazableString is explicitly handled in this file already (strcmp),
		// and gives direct access to its underlying byte[].
		if (s instanceof ReplazableString rs) {
			return rs.buffer;
		}
		// CompactString implements ByteString in this codebase, so ByteString
		// covers it too.
		if (s instanceof ByteString bs) {
			return bs.getBuffer();
		}
		return null;
	}

	private static int lcpMismatch(byte[] a, byte[] b, int from, int len) {
		// Range mismatch returns the *relative* mismatch index, or -1 if equal.
		final int mismatch = Arrays.mismatch(a, from, len, b, from, len);
		return mismatch < 0 ? (len - from) : mismatch;
	}

	public static int strcmp(CharSequence str, byte[] buff2, int off2) {
		byte[] buff1;
		int off1;
		int len1;
		int len2 = buff2.length;

		if (str instanceof CompactString) {
			buff1 = ((CompactString) str).getData();
			off1 = 0;
			len1 = buff1.length;
		} else if (str instanceof String) {
			buff1 = ((String) str).getBytes(ByteStringUtil.STRING_ENCODING);
			off1 = 0;
			len1 = buff1.length;
		} else if (str instanceof ReplazableString) {
			buff1 = ((ReplazableString) str).buffer;
			off1 = 0;
			len1 = ((ReplazableString) str).used;
		} else {
			throw new NotImplementedException();
		}

		int n = Math.min(len1 - off1, len2 - off2);

		int p1 = off1;
		int p2 = off2;
		while (n-- != 0) {
			int a = buff1[p1++] & 0xFF;
			int b = buff2[p2++] & 0xFF;
			if (a != b) {
				return a - b;
			}
			if (a == 0) {
				return 0;
			}
		}

		if (p1 - off1 < len1 && buff1[p1] != 0) {
			// Still remaining in string one, second is shorter
			return 1;
		}
		if (p2 - off2 < len2 && buff2[p2] != 0) {
			// Still remaining in string two, first is shorter.
			return -1;
		}
		return 0;
	}

	public static int strcmp(CharSequence str, BigByteBuffer buff2, long off2) {
		byte[] buff1;
		int off1;
		long len1;
		long len2 = buff2.size();

		if (str instanceof CompactString) {
			buff1 = ((CompactString) str).getData();
			off1 = 0;
			len1 = buff1.length;
		} else if (str instanceof String) {
			buff1 = ((String) str).getBytes(ByteStringUtil.STRING_ENCODING);
			off1 = 0;
			len1 = buff1.length;
		} else if (str instanceof ReplazableString) {
			buff1 = ((ReplazableString) str).buffer;
			off1 = 0;
			len1 = ((ReplazableString) str).used;
		} else {
			throw new NotImplementedException();
		}

		int n = (int) Math.min(len1 - off1, len2 - off2);

		int p1 = off1;
		long p2 = off2;
		while (n-- != 0) {
			int a = buff1[p1++] & 0xFF;
			int b = buff2.get(p2++) & 0xFF;
			if (a != b) {
				return a - b;
			}
			if (a == 0) {
				return 0;
			}
		}

		if (p1 - off1 < len1 && buff1[p1] != 0) {
			// Still remaining in string one, second is shorter
			return 1;
		}
		if (p2 - off2 < len2 && buff2.get(p2) != 0) {
			// Still remaining in string two, first is shorter.
			return -1;
		}
		return 0;
	}

	public static int strcmp(CharSequence str, ByteBuffer buffer, int offset) {
		byte[] buf;
		int len;

		str = DelayedString.unwrap(str);

		// Isolate array
		if (str instanceof CompactString) {
			buf = ((CompactString) str).getData();
			len = buf.length;
		} else if (str instanceof String) {
			buf = ((String) str).getBytes(ByteStringUtil.STRING_ENCODING);
			len = buf.length;
		} else if (str instanceof ReplazableString) {
			buf = ((ReplazableString) str).buffer;
			len = ((ReplazableString) str).used;
		} else {
			throw new NotImplementedException();
		}

		// Compare
		int i = 0;
		long n = Math.min(len, buffer.capacity() - offset);
		while (i < n) {
			int v1 = buf[i] & 0xFF;
			int v2 = buffer.get(offset + i) & 0xFF;

			if (v1 != v2) {
				return v1 - v2;
			}
			if (v1 == 0) {
				return 0;
			}
			i++;
		}

		// One of the buffer exhausted
		if (buffer.capacity() - offset - i > 0) {
			byte v = buffer.get(offset + i);
			if (v == 0) {
				return 0;
			} else {
				return -1;
			}
		} else {
			throw new IllegalArgumentException("Buffer is not Null-Terminated");
		}
	}

	public static int strcmp(CharSequence str, BigMappedByteBuffer buffer, long offset) {
		byte[] buf;
		int len;

		str = DelayedString.unwrap(str);

		// Isolate array
		if (str instanceof CompactString) {
			buf = ((CompactString) str).getData();
			len = buf.length;
		} else if (str instanceof String) {
			buf = ((String) str).getBytes(ByteStringUtil.STRING_ENCODING);
			len = buf.length;
		} else if (str instanceof ReplazableString) {
			buf = ((ReplazableString) str).buffer;
			len = ((ReplazableString) str).used;
		} else {
			throw new NotImplementedException();
		}

		// Compare
		int i = 0;
		long n = Math.min(len, buffer.capacity() - offset);
		while (i < n) {
			int v1 = buf[i] & 0xFF;
			int v2 = buffer.get(offset + i) & 0xFF;

			if (v1 != v2) {
				return v1 - v2;
			}
			if (v1 == 0) {
				return 0;
			}
			i++;
		}

		// One of the buffer exhausted
		if (buffer.capacity() - offset - i > 0) {
			byte v = buffer.get(offset + i);
			if (v == 0) {
				return 0;
			} else {
				return -1;
			}
		} else {
			throw new IllegalArgumentException("Buffer is not Null-Terminated");
		}
	}

	public static int append(OutputStream out, CharSequence str, int start) throws IOException {
		if (str instanceof DelayedString) {
			str = ((DelayedString) str).getInternal();
		}

		if (str instanceof String) {
			return append(out, (String) str, start);
		} else if (str instanceof ByteString) {
			return append(out, (ByteString) str, start);
		} else {
			throw new NotImplementedException();
		}
	}

	public static int append(OutputStream out, ByteString str, int start) throws IOException {
		return append(out, str.getBuffer(), start, str.length());
	}

	public static int append(OutputStream out, String str, int start) throws IOException {
		byte[] bytes = str.getBytes(ByteStringUtil.STRING_ENCODING);
		return append(out, bytes, start, bytes.length);
	}

	public static int append(OutputStream out, byte[] bytes, int start, int len) throws IOException {
		// Write and remove null characters
		int cur = start;
		int ini = start;
		int written = 0;

		while (cur < len) {
			if (bytes[cur] == 0) {
				out.write(bytes, ini, cur - ini);
				written += (cur - ini);
				ini = cur + 1;
			}
			cur++;
		}
		if (ini < len) {
			out.write(bytes, ini, len - ini);
			written += (len - ini);
		}
		return written;
	}

}
