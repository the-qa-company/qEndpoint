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
import java.nio.charset.Charset;

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
		int len = Math.min(str1.length(), str2.length());
		int delta = from;
		while (delta < len && str1.charAt(delta) == str2.charAt(delta)) {
			delta++;
		}
		return delta - from;
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
