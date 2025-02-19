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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.util.io.BigByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.BigMappedByteBuffer;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import org.apache.jena.base.Sys;

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

//	public static int longestCommonPrefix(CharSequence str1, CharSequence str2, int from) {
//		int len = Math.min(str1.length(), str2.length());
//		int delta = from;
//		while (delta < len && str1.charAt(delta) == str2.charAt(delta)) {
//			delta++;
//		}
//		return delta - from;
//	}

	public static void main(String[] args) {
		ArrayList<byte[]> bytes1 = new ArrayList<>();
		ArrayList<byte[]> bytes2 = new ArrayList<>();

		Random random = new Random();

		for (int i = 0; i < 2048 * 32; i++) {
			byte[] byteArray = new byte[random.nextInt(2048)];
			for (int j = 0; j < byteArray.length; j++) {
				byteArray[j] = (byte) random.nextInt();
			}
			bytes1.add(byteArray);
		}

		for (int i = 0; i < 2048 * 32; i++) {
			byte[] byteArray = new byte[random.nextInt(2048)];
			for (int j = 0; j < byteArray.length; j++) {
				byteArray[j] = (byte) random.nextInt();
			}
			bytes2.add(byteArray);
		}

		int[] millis = new int[2048];
		int[] mismatch = new int[2048];

		for (int k = 0; k < 10; k++) {
			for (int i = 0; i < bytes1.size(); i++) {
				byte[] byteArray1 = bytes1.get(i);
				byte[] byteArray2 = bytes2.get(i);

				long start = System.nanoTime();
				int mismatch1 = Arrays.mismatch(byteArray1, byteArray2);
				long l = System.nanoTime();
				if (millis[Math.min(byteArray1.length, byteArray2.length)] == 0) {
					millis[Math.min(byteArray1.length, byteArray2.length)] = (int) (l - start);
					mismatch[Math.min(byteArray1.length, byteArray2.length)] = mismatch1;
				} else {
					millis[Math.min(byteArray1.length, byteArray2.length)] = Math
							.min(millis[Math.min(byteArray1.length, byteArray2.length)], (int) (l - start));
					mismatch[Math.min(byteArray1.length, byteArray2.length)] = mismatch1;
				}
			}
		}

		for (int i = 0; i < millis.length; i++) {
			System.out.println(i + " " + millis[i] + " ns " + mismatch[i]);
		}

	}

	public static int longestCommonPrefix(CharSequence str1, CharSequence str2, int from) {

		int len = Math.min(str1.length(), str2.length());
		if (from >= len) {
			return 0;
		}

		if (str1.charAt(from) != str2.charAt(from)) {
			return 0;
		}

		if (str1 instanceof ByteString && str2 instanceof ByteString) {

			if (len - from < 4) {
				if (from == 0) {
					return switch (len) {
					case 1 -> str1.charAt(0) == str2.charAt(0) ? 1 : 0;
					case 2 -> naiveLength2(str1, str2);
					case 3 -> naiveLength3(str1, str2);
					default -> naive(str1, str2, from, len);
					};
				}
				return naive(str1, str2, from, len);
			}

			byte[] buffer = ((ByteString) str1).getBuffer();
			byte[] buffer2 = ((ByteString) str2).getBuffer();
			return mismatch(from, buffer, len, buffer2);

		}

		return naive(str1, str2, from, len);
	}

	private static int naiveLength3(CharSequence str1, CharSequence str2) {
		char c = str1.charAt(0);
		char c1 = str2.charAt(0);
		if (c != c1) {
			return 0;
		}
		c = str1.charAt(1);
		c1 = str2.charAt(1);
		if (c != c1) {
			return 1;
		}
		c = str1.charAt(2);
		c1 = str2.charAt(2);
		return c == c1 ? 3 : 2;
	}

	private static int naiveLength2(CharSequence str1, CharSequence str2) {
		char c = str1.charAt(0);
		char c1 = str2.charAt(0);
		if (c != c1) {
			return 0;
		}
		c = str1.charAt(1);
		c1 = str2.charAt(1);
		return c == c1 ? 2 : 1;
	}

	private static int mismatch(int from, byte[] buffer, int len, byte[] buffer2) {
		int missmatch = Arrays.mismatch(buffer, from, len, buffer2, from, len);
		if (missmatch == -1) {
			return len - from;
		} else {
			return missmatch;
		}
	}

	private static int naive(CharSequence str1, CharSequence str2, int from, int len) {
		int delta = from;
		while (delta < len && str1.charAt(delta) == str2.charAt(delta)) {
			delta++;
		}
		return delta - from;
	}

	private static int naive(CharSequence str1, CharSequence str2) {
		int delta = 0;
		while (delta < 3 && str1.charAt(delta) == str2.charAt(delta)) {
			delta++;
		}
		return delta;
	}

	private static int mismatchVectorByte(byte[] byteData1, byte[] byteData2) {
		int length = Math.min(byteData1.length, byteData2.length);
		int index = 0;
		for (; index < ByteVector.SPECIES_PREFERRED.loopBound(length); index += ByteVector.SPECIES_PREFERRED.length()) {
			ByteVector vector1 = ByteVector.fromArray(ByteVector.SPECIES_PREFERRED, byteData1, index);
			ByteVector vector2 = ByteVector.fromArray(ByteVector.SPECIES_PREFERRED, byteData2, index);
			VectorMask<Byte> mask = vector1.compare(VectorOperators.NE, vector2);
			if (mask.anyTrue()) {
				return index + mask.firstTrue();
			}
		}
		// process the tail
		int mismatch = -1;
		for (int i = index; i < length; ++i) {
			if (byteData1[i] != byteData2[i]) {
				mismatch = i;
				break;
			}
		}
		return mismatch;
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
