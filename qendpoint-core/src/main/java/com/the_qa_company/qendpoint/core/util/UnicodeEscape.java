package com.the_qa_company.qendpoint.core.util;

import com.the_qa_company.qendpoint.core.util.io.BigMappedByteBuffer;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.ByteStringInterner;
import com.the_qa_company.qendpoint.core.util.string.CompactString;

import java.io.IOException;

public class UnicodeEscape {

	private UnicodeEscape() {
	}

	/**
	 * Checks whether the supplied character is a letter or number according to
	 * the N-Triples specification.
	 *
	 * @see #isLetter
	 * @see #isNumber
	 * @return boolean
	 */
	private static boolean isLetterOrNumber(int c) {
		return isLetter(c) || isNumber(c);
	}

	/**
	 * Checks whether the supplied character is a letter according to the
	 * N-Triples specification. N-Triples letters are A - Z and a - z.
	 *
	 * @param c character
	 * @return boolean
	 */
	private static boolean isLetter(int c) {
		return (c >= 65 && c <= 90) || // A - Z
				(c >= 97 && c <= 122); // a - z
	}

	/**
	 * Checks whether the supplied character is a number according to the
	 * N-Triples specification. N-Triples numbers are 0 - 9.
	 *
	 * @param c character
	 * @return boolean
	 */
	private static boolean isNumber(int c) {
		return c >= 48 && c <= 57; // 0 - 9
	}

	/**
	 * Escapes a Unicode string to an all-ASCII character sequence. Any special
	 * characters are escaped using backslashes (<code>"</code> becomes
	 * <code>\"</code>, etc.), and non-ascii/non-printable characters are
	 * escaped using Unicode escapes (<code>&#x5C;uxxxx</code> and
	 * <code>&#x5C;Uxxxxxxxx</code>).
	 *
	 * @param label label
	 * @return String
	 */
	public static String escapeString(String label) {
		try {
			StringBuilder sb = new StringBuilder(2 * label.length());
			escapeString(label, sb);
			return sb.toString();
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	/**
	 * Escapes a Unicode string to an all-ASCII character sequence. Any special
	 * characters are escaped using backslashes (<code>"</code> becomes
	 * <code>\"</code>, etc.), and non-ascii/non-printable characters are
	 * escaped using Unicode escapes (<code>&#x5C;uxxxx</code> and
	 * <code>&#x5C;Uxxxxxxxx</code>).
	 *
	 * @param label      label
	 * @param appendable appendable
	 * @throws IOException when IOException occurs
	 */
	public static void escapeString(String label, Appendable appendable) throws IOException {

		int first = 0;
		int last = label.length();

		if (last > 1 && label.charAt(0) == '<' && label.charAt(last - 1) == '>') {
			first++;
			last--;
		} else if (label.charAt(0) == '"') {
			first = 1;
			appendable.append('"');

			for (int i = last - 1; i > 0; i--) {
				char curr = label.charAt(i);

				if (curr == '"') {
					// The datatype or lang must be after the last " symbol.
					last = i - 1;
					break;
				}

				char prev = label.charAt(i - 1);
				if (curr == '@' && prev == '"') {
					last = i - 2;
					break;
				}
				if (curr == '^' && prev == '^') {
					last = i - 3;
					break;
				}
			}
		}

		if (last == label.length()) {
			last--;
		}

		for (int i = first; i <= last; i++) {
			char c = label.charAt(i);
			int cInt = c;

			if (c == '\\') {
				appendable.append("\\\\");
			} else if (c == '"') {
				appendable.append("\\\"");
			} else if (c == '\n') {
				appendable.append("\\n");
			} else if (c == '\r') {
				appendable.append("\\r");
			} else if (c == '\t') {
				appendable.append("\\t");
			} else if (cInt >= 0x0 && cInt <= 0x8 || cInt == 0xB || cInt == 0xC || cInt >= 0xE && cInt <= 0x1F
					|| cInt >= 0x7F && cInt <= 0xFFFF) {
				appendable.append("\\u");
				appendable.append(toHexString(cInt, 4));
			} else if (cInt >= 0x10000 && cInt <= 0x10FFFF) {
				appendable.append("\\U");
				appendable.append(toHexString(cInt, 8));
			} else {
				appendable.append(c);
			}
		}

		appendable.append(label.subSequence(last + 1, label.length()));
	}

	/**
	 * Unescapes an escaped Unicode string. Any Unicode sequences
	 * (<code>&#x5C;uxxxx</code> and <code>&#x5C;Uxxxxxxxx</code>) are restored
	 * to the value indicated by the hexadecimal argument and any
	 * backslash-escapes (<code>\"</code>, <code>\\</code>, etc.) are decoded to
	 * their original form.
	 *
	 * @param s An escaped Unicode string.
	 * @return The unescaped string.
	 * @throws IllegalArgumentException If the supplied string is not a
	 *                                  correctly escaped N-Triples string.
	 */
	public static String unescapeString(String s) {
		return unescapeString(s, 0, s.length());
	}

	/**
	 * Unescapes an escaped Unicode string. Any Unicode sequences
	 * (<code>&#x5C;uxxxx</code> and <code>&#x5C;Uxxxxxxxx</code>) are restored
	 * to the value indicated by the hexadecimal argument and any
	 * backslash-escapes (<code>\"</code>, <code>\\</code>, etc.) are decoded to
	 * their original form.
	 *
	 * @param s An escaped Unicode string.
	 * @return The unescaped string.
	 * @throws IllegalArgumentException If the supplied string is not a
	 *                                  correctly escaped N-Triples string.
	 */
	public static String unescapeString(String s, int start, int sLength) {
		int backSlashIdx = s.indexOf('\\', start);

		if (backSlashIdx == -1 || backSlashIdx >= sLength) {
			// No escaped characters found
			return s.substring(start, sLength);
		}

		int startIdx = start;
		StringBuilder sb = new StringBuilder(sLength);

		while (backSlashIdx != -1 && backSlashIdx < sLength) {
			sb.append(s, startIdx, backSlashIdx);

			if (backSlashIdx + 1 >= sLength) {
				throw new IllegalArgumentException("Unescaped backslash in: " + s);
			}

			char c = s.charAt(backSlashIdx + 1);

			if (c == 't') {
				sb.append('\t');
				startIdx = backSlashIdx + 2;
			} else if (c == 'r') {
				sb.append('\r');
				startIdx = backSlashIdx + 2;
			} else if (c == 'n') {
				sb.append('\n');
				startIdx = backSlashIdx + 2;
			} else if (c == '"') {
				sb.append('"');
				startIdx = backSlashIdx + 2;
			} else if (c == '\\') {
				sb.append('\\');
				startIdx = backSlashIdx + 2;
			} else if (c == 'u') {
				// not canonical but whatever
				// \\uxxxx
				if (backSlashIdx + 5 >= sLength) {
					throw new IllegalArgumentException("Incomplete Unicode escape sequence in: " + s);
				}
				String xx = s.substring(backSlashIdx + 2, backSlashIdx + 6);

				try {
					c = (char) Integer.parseInt(xx, 16);
					sb.append(c);

					startIdx = backSlashIdx + 6;
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException("Illegal Unicode escape sequence '\\u" + xx + "' in: " + s);
				}
			} else if (c == 'U') {
				// not canonical but whatever
				// \\Uxxxxxxxx
				if (backSlashIdx + 9 >= sLength) {
					throw new IllegalArgumentException("Incomplete Unicode escape sequence in: " + s);
				}
				String xx = s.substring(backSlashIdx + 2, backSlashIdx + 10);

				try {
					char[] chars = Character.toChars(Integer.parseInt(xx, 16));
					for (char cc : chars) {
						sb.append(cc);
					}

					startIdx = backSlashIdx + 10;
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException("Illegal Unicode escape sequence '\\U" + xx + "' in: " + s);
				}
			} else {
				throw new IllegalArgumentException("Unescaped backslash in: " + s);
			}

			backSlashIdx = s.indexOf('\\', startIdx);
		}

		sb.append(s, startIdx, sLength);

		return sb.toString();
	}

	/**
	 * Unescapes an escaped Unicode string into a {@link ByteString}. The
	 * resulting bytes are UTF-8 encoded and match the behavior of:
	 *
	 * <pre>
	 * new CompactString(unescapeString(s, start, end))
	 * </pre>
	 *
	 * but without allocating the intermediate {@link String}.
	 *
	 * @param s     escaped input string
	 * @param start start index (inclusive)
	 * @param end   end index (exclusive)
	 * @return unescaped UTF-8 {@link ByteString}
	 */
	public static ByteString unescapeByteString(String s, int start, int end) {
		int backSlashIdx = s.indexOf('\\', start);

		if (backSlashIdx == -1 || backSlashIdx >= end) {
			// Fast path: no escaped characters.
			return utf8EncodeRange(s, start, end);
		}

		int byteLen = utf8LengthAfterUnescape(s, start, end);
		byte[] out = new byte[byteLen];
		int written = writeUtf8AfterUnescape(s, start, end, out, 0);
		assert written == byteLen;
		return new CompactString(out);
	}

	public static ByteString unescapeByteString(String s, int start, int end, ByteStringInterner interner) {
		if (interner == null) {
			return unescapeByteString(s, start, end);
		}

		int backSlashIdx = s.indexOf('\\', start);
		if (backSlashIdx == -1 || backSlashIdx >= end) {
			return utf8EncodeRange(s, start, end, interner);
		}

		int byteLen = utf8LengthAfterUnescape(s, start, end);
		byte[] scratch = interner.ensureScratchCapacity(byteLen);
		int written = writeUtf8AfterUnescape(s, start, end, scratch, 0);
		assert written == byteLen;
		return interner.internScratch(scratch, written);
	}

	/**
	 * Unescapes an escaped Unicode string stored in a character buffer into a
	 * {@link ByteString} without allocating an intermediate {@link String}.
	 *
	 * @param buffer escaped input buffer
	 * @param start  start index (inclusive)
	 * @param end    end index (exclusive)
	 * @return unescaped UTF-8 {@link ByteString}
	 */
	public static ByteString unescapeByteString(char[] buffer, int start, int end) {
		int backSlashIdx = indexOf(buffer, start, end, '\\');

		if (backSlashIdx == -1) {
			return utf8EncodeRange(buffer, start, end);
		}

		int byteLen = utf8LengthAfterUnescape(buffer, start, end);
		byte[] out = new byte[byteLen];
		int written = writeUtf8AfterUnescape(buffer, start, end, out, 0);
		assert written == byteLen;
		return new CompactString(out);
	}

	public static ByteString unescapeByteString(char[] buffer, int start, int end, ByteStringInterner interner) {
		if (interner == null) {
			return unescapeByteString(buffer, start, end);
		}

		int backSlashIdx = indexOf(buffer, start, end, '\\');
		if (backSlashIdx == -1) {
			return utf8EncodeRange(buffer, start, end, interner);
		}

		int byteLen = utf8LengthAfterUnescape(buffer, start, end);
		byte[] scratch = interner.ensureScratchCapacity(byteLen);
		int written = writeUtf8AfterUnescape(buffer, start, end, scratch, 0);
		assert written == byteLen;
		return interner.internScratch(scratch, written);
	}

	/**
	 * Unescapes an escaped UTF-8 sequence from a {@link BigMappedByteBuffer},
	 * producing a {@link ByteString} without allocating an intermediate
	 * {@link String}.
	 *
	 * @param buffer UTF-8 bytes
	 * @param start  start index (inclusive)
	 * @param end    end index (exclusive)
	 * @return unescaped UTF-8 {@link ByteString}
	 */
	public static ByteString unescapeByteString(BigMappedByteBuffer buffer, long start, long end) {
		if (start >= end) {
			return ByteString.empty();
		}

		long backSlashIdx = indexOf(buffer, start, end, (byte) '\\');
		if (backSlashIdx < 0) {
			int len = Math.toIntExact(end - start);
			byte[] out = new byte[len];
			for (int i = 0; i < len; i++) {
				out[i] = buffer.get(start + i);
			}
			return new CompactString(out);
		}

		int byteLen = utf8LengthAfterUnescape(buffer, start, end);
		byte[] out = new byte[byteLen];
		int written = writeUtf8AfterUnescape(buffer, start, end, out, 0);
		assert written == byteLen;
		return new CompactString(out);
	}

	public static ByteString unescapeByteString(BigMappedByteBuffer buffer, long start, long end,
			ByteStringInterner interner) {
		if (interner == null) {
			return unescapeByteString(buffer, start, end);
		}
		if (start >= end) {
			return ByteString.empty();
		}

		long backSlashIdx = indexOf(buffer, start, end, (byte) '\\');
		if (backSlashIdx < 0) {
			int len = Math.toIntExact(end - start);
			byte[] scratch = interner.ensureScratchCapacity(len);
			for (int i = 0; i < len; i++) {
				scratch[i] = buffer.get(start + i);
			}
			return interner.internScratch(scratch, len);
		}

		int byteLen = utf8LengthAfterUnescape(buffer, start, end);
		byte[] scratch = interner.ensureScratchCapacity(byteLen);
		int written = writeUtf8AfterUnescape(buffer, start, end, scratch, 0);
		assert written == byteLen;
		return interner.internScratch(scratch, written);
	}

	private static ByteString utf8EncodeRange(String s, int start, int end) {
		// Common case: ASCII-only
		int len = end - start;
		byte[] out = new byte[len];
		for (int i = 0; i < len; i++) {
			char c = s.charAt(start + i);
			if (c >= 0x80) {
				return utf8EncodeRangeSlow(s, start, end);
			}
			out[i] = (byte) c;
		}
		return new CompactString(out);
	}

	private static ByteString utf8EncodeRange(String s, int start, int end, ByteStringInterner interner) {
		int len = end - start;
		byte[] scratch = interner.ensureScratchCapacity(len);
		for (int i = 0; i < len; i++) {
			char c = s.charAt(start + i);
			if (c >= 0x80) {
				return utf8EncodeRangeSlow(s, start, end, interner);
			}
			scratch[i] = (byte) c;
		}
		return interner.internScratch(scratch, len);
	}

	private static ByteString utf8EncodeRange(char[] buffer, int start, int end) {
		int len = end - start;
		byte[] out = new byte[len];
		for (int i = 0; i < len; i++) {
			char c = buffer[start + i];
			if (c >= 0x80) {
				return utf8EncodeRangeSlow(buffer, start, end);
			}
			out[i] = (byte) c;
		}
		return new CompactString(out);
	}

	private static ByteString utf8EncodeRange(char[] buffer, int start, int end, ByteStringInterner interner) {
		int len = end - start;
		byte[] scratch = interner.ensureScratchCapacity(len);
		for (int i = 0; i < len; i++) {
			char c = buffer[start + i];
			if (c >= 0x80) {
				return utf8EncodeRangeSlow(buffer, start, end, interner);
			}
			scratch[i] = (byte) c;
		}
		return interner.internScratch(scratch, len);
	}

	private static ByteString utf8EncodeRangeSlow(String s, int start, int end) {
		int byteLen = utf8LengthRaw(s, start, end);
		byte[] out = new byte[byteLen];
		int written = writeUtf8Raw(s, start, end, out, 0);
		assert written == byteLen;
		return new CompactString(out);
	}

	private static ByteString utf8EncodeRangeSlow(String s, int start, int end, ByteStringInterner interner) {
		int byteLen = utf8LengthRaw(s, start, end);
		byte[] scratch = interner.ensureScratchCapacity(byteLen);
		int written = writeUtf8Raw(s, start, end, scratch, 0);
		assert written == byteLen;
		return interner.internScratch(scratch, written);
	}

	private static ByteString utf8EncodeRangeSlow(char[] buffer, int start, int end) {
		int byteLen = utf8LengthRaw(buffer, start, end);
		byte[] out = new byte[byteLen];
		int written = writeUtf8Raw(buffer, start, end, out, 0);
		assert written == byteLen;
		return new CompactString(out);
	}

	private static ByteString utf8EncodeRangeSlow(char[] buffer, int start, int end, ByteStringInterner interner) {
		int byteLen = utf8LengthRaw(buffer, start, end);
		byte[] scratch = interner.ensureScratchCapacity(byteLen);
		int written = writeUtf8Raw(buffer, start, end, scratch, 0);
		assert written == byteLen;
		return interner.internScratch(scratch, written);
	}

	private static int utf8LengthRaw(String s, int start, int end) {
		int len = 0;
		int pendingHigh = -1;

		for (int i = start; i < end; i++) {
			char c = s.charAt(i);

			if (pendingHigh != -1) {
				if (Character.isLowSurrogate(c)) {
					int cp = Character.toCodePoint((char) pendingHigh, c);
					len += utf8LengthCodePoint(cp);
					pendingHigh = -1;
					continue;
				}
				len += utf8LengthCodePoint('?');
				pendingHigh = -1;
			}

			if (Character.isHighSurrogate(c)) {
				pendingHigh = c;
			} else if (Character.isLowSurrogate(c)) {
				len += utf8LengthCodePoint('?');
			} else {
				len += utf8LengthCodePoint(c);
			}
		}

		if (pendingHigh != -1) {
			len += utf8LengthCodePoint('?');
		}

		return len;
	}

	private static int utf8LengthRaw(char[] buffer, int start, int end) {
		int len = 0;
		int pendingHigh = -1;

		for (int i = start; i < end; i++) {
			char c = buffer[i];

			if (pendingHigh != -1) {
				if (Character.isLowSurrogate(c)) {
					int cp = Character.toCodePoint((char) pendingHigh, c);
					len += utf8LengthCodePoint(cp);
					pendingHigh = -1;
					continue;
				}
				len += utf8LengthCodePoint('?');
				pendingHigh = -1;
			}

			if (Character.isHighSurrogate(c)) {
				pendingHigh = c;
			} else if (Character.isLowSurrogate(c)) {
				len += utf8LengthCodePoint('?');
			} else {
				len += utf8LengthCodePoint(c);
			}
		}

		if (pendingHigh != -1) {
			len += utf8LengthCodePoint('?');
		}

		return len;
	}

	private static int writeUtf8Raw(String s, int start, int end, byte[] out, int off) {
		int pendingHigh = -1;

		for (int i = start; i < end; i++) {
			char c = s.charAt(i);

			if (pendingHigh != -1) {
				if (Character.isLowSurrogate(c)) {
					int cp = Character.toCodePoint((char) pendingHigh, c);
					off = writeUtf8CodePoint(cp, out, off);
					pendingHigh = -1;
					continue;
				}
				off = writeUtf8CodePoint('?', out, off);
				pendingHigh = -1;
			}

			if (Character.isHighSurrogate(c)) {
				pendingHigh = c;
			} else if (Character.isLowSurrogate(c)) {
				off = writeUtf8CodePoint('?', out, off);
			} else {
				off = writeUtf8CodePoint(c, out, off);
			}
		}

		if (pendingHigh != -1) {
			off = writeUtf8CodePoint('?', out, off);
		}

		return off;
	}

	private static int writeUtf8Raw(char[] buffer, int start, int end, byte[] out, int off) {
		int pendingHigh = -1;

		for (int i = start; i < end; i++) {
			char c = buffer[i];

			if (pendingHigh != -1) {
				if (Character.isLowSurrogate(c)) {
					int cp = Character.toCodePoint((char) pendingHigh, c);
					off = writeUtf8CodePoint(cp, out, off);
					pendingHigh = -1;
					continue;
				}
				off = writeUtf8CodePoint('?', out, off);
				pendingHigh = -1;
			}

			if (Character.isHighSurrogate(c)) {
				pendingHigh = c;
			} else if (Character.isLowSurrogate(c)) {
				off = writeUtf8CodePoint('?', out, off);
			} else {
				off = writeUtf8CodePoint(c, out, off);
			}
		}

		if (pendingHigh != -1) {
			off = writeUtf8CodePoint('?', out, off);
		}

		return off;
	}

	private static int utf8LengthAfterUnescape(String s, int start, int end) {
		int len = 0;
		int pendingHigh = -1;
		int i = start;

		while (i < end) {
			char c = s.charAt(i);

			if (c != '\\') {
				if (pendingHigh != -1) {
					if (Character.isLowSurrogate(c)) {
						int cp = Character.toCodePoint((char) pendingHigh, c);
						len += utf8LengthCodePoint(cp);
						pendingHigh = -1;
						i++;
						continue;
					}
					len += utf8LengthCodePoint('?');
					pendingHigh = -1;
				}

				if (Character.isHighSurrogate(c)) {
					pendingHigh = c;
				} else if (Character.isLowSurrogate(c)) {
					len += utf8LengthCodePoint('?');
				} else {
					len += utf8LengthCodePoint(c);
				}
				i++;
				continue;
			}

			if (i + 1 >= end) {
				throw new IllegalArgumentException("Unescaped backslash in: " + s);
			}

			char esc = s.charAt(i + 1);
			switch (esc) {
			case 't' -> len = utf8LengthEmitChar('\t', pendingHigh, len);
			case 'r' -> len = utf8LengthEmitChar('\r', pendingHigh, len);
			case 'n' -> len = utf8LengthEmitChar('\n', pendingHigh, len);
			case '"' -> len = utf8LengthEmitChar('"', pendingHigh, len);
			case '\\' -> len = utf8LengthEmitChar('\\', pendingHigh, len);
			case 'u' -> {
				if (i + 5 >= end) {
					throw new IllegalArgumentException("Incomplete Unicode escape sequence in: " + s);
				}
				int cp = parseHex4(s, i + 2, end, 'u');
				char decoded = (char) cp;

				if (pendingHigh != -1) {
					if (Character.isLowSurrogate(decoded)) {
						len += utf8LengthCodePoint(Character.toCodePoint((char) pendingHigh, decoded));
						pendingHigh = -1;
					} else {
						len += utf8LengthCodePoint('?');
						pendingHigh = Character.isHighSurrogate(decoded) ? decoded : -1;
						if (pendingHigh == -1) {
							len += utf8LengthEmitNonSurrogateChar(decoded);
						}
					}
				} else if (Character.isHighSurrogate(decoded)) {
					pendingHigh = decoded;
				} else if (Character.isLowSurrogate(decoded)) {
					len += utf8LengthCodePoint('?');
				} else {
					len += utf8LengthCodePoint(decoded);
				}
				i += 6;
				continue;
			}
			case 'U' -> {
				if (i + 9 >= end) {
					throw new IllegalArgumentException("Incomplete Unicode escape sequence in: " + s);
				}
				int cp = parseHex8ToInt(s, i + 2, end, 'U');
				char[] chars = Character.toChars(cp);
				for (char decoded : chars) {
					if (pendingHigh != -1) {
						if (Character.isLowSurrogate(decoded)) {
							len += utf8LengthCodePoint(Character.toCodePoint((char) pendingHigh, decoded));
							pendingHigh = -1;
							continue;
						}
						len += utf8LengthCodePoint('?');
						pendingHigh = -1;
					}
					if (Character.isHighSurrogate(decoded)) {
						pendingHigh = decoded;
					} else if (Character.isLowSurrogate(decoded)) {
						len += utf8LengthCodePoint('?');
					} else {
						len += utf8LengthCodePoint(decoded);
					}
				}
				i += 10;
				continue;
			}
			default -> throw new IllegalArgumentException("Unescaped backslash in: " + s);
			}

			if (esc == 't' || esc == 'r' || esc == 'n' || esc == '"' || esc == '\\') {
				i += 2;
				pendingHigh = -1;
				continue;
			}
		}

		if (pendingHigh != -1) {
			len += utf8LengthCodePoint('?');
		}

		return len;
	}

	private static int utf8LengthAfterUnescape(char[] buffer, int start, int end) {
		int len = 0;
		int pendingHigh = -1;
		int i = start;

		while (i < end) {
			char c = buffer[i];

			if (c != '\\') {
				if (pendingHigh != -1) {
					if (Character.isLowSurrogate(c)) {
						int cp = Character.toCodePoint((char) pendingHigh, c);
						len += utf8LengthCodePoint(cp);
						pendingHigh = -1;
						i++;
						continue;
					}
					len += utf8LengthCodePoint('?');
					pendingHigh = -1;
				}

				if (Character.isHighSurrogate(c)) {
					pendingHigh = c;
				} else if (Character.isLowSurrogate(c)) {
					len += utf8LengthCodePoint('?');
				} else {
					len += utf8LengthCodePoint(c);
				}
				i++;
				continue;
			}

			if (i + 1 >= end) {
				throw new IllegalArgumentException("Unescaped backslash in input buffer");
			}

			char esc = buffer[i + 1];
			switch (esc) {
			case 't' -> len = utf8LengthEmitChar('\t', pendingHigh, len);
			case 'r' -> len = utf8LengthEmitChar('\r', pendingHigh, len);
			case 'n' -> len = utf8LengthEmitChar('\n', pendingHigh, len);
			case '"' -> len = utf8LengthEmitChar('"', pendingHigh, len);
			case '\\' -> len = utf8LengthEmitChar('\\', pendingHigh, len);
			case 'u' -> {
				if (i + 5 >= end) {
					throw new IllegalArgumentException("Incomplete Unicode escape sequence in input buffer");
				}
				int cp = parseHex4(buffer, i + 2, end, 'u');
				char decoded = (char) cp;

				if (pendingHigh != -1) {
					if (Character.isLowSurrogate(decoded)) {
						len += utf8LengthCodePoint(Character.toCodePoint((char) pendingHigh, decoded));
						pendingHigh = -1;
					} else {
						len += utf8LengthCodePoint('?');
						pendingHigh = Character.isHighSurrogate(decoded) ? decoded : -1;
						if (pendingHigh == -1) {
							len += utf8LengthEmitNonSurrogateChar(decoded);
						}
					}
				} else if (Character.isHighSurrogate(decoded)) {
					pendingHigh = decoded;
				} else if (Character.isLowSurrogate(decoded)) {
					len += utf8LengthCodePoint('?');
				} else {
					len += utf8LengthCodePoint(decoded);
				}
				i += 6;
				continue;
			}
			case 'U' -> {
				if (i + 9 >= end) {
					throw new IllegalArgumentException("Incomplete Unicode escape sequence in input buffer");
				}
				int cp = parseHex8ToInt(buffer, i + 2, end, 'U');
				char[] chars = Character.toChars(cp);
				for (char decoded : chars) {
					if (pendingHigh != -1) {
						if (Character.isLowSurrogate(decoded)) {
							len += utf8LengthCodePoint(Character.toCodePoint((char) pendingHigh, decoded));
							pendingHigh = -1;
							continue;
						}
						len += utf8LengthCodePoint('?');
						pendingHigh = -1;
					}
					if (Character.isHighSurrogate(decoded)) {
						pendingHigh = decoded;
					} else if (Character.isLowSurrogate(decoded)) {
						len += utf8LengthCodePoint('?');
					} else {
						len += utf8LengthCodePoint(decoded);
					}
				}
				i += 10;
				continue;
			}
			default -> throw new IllegalArgumentException("Unescaped backslash in input buffer");
			}

			if (esc == 't' || esc == 'r' || esc == 'n' || esc == '"' || esc == '\\') {
				i += 2;
				pendingHigh = -1;
				continue;
			}
		}

		if (pendingHigh != -1) {
			len += utf8LengthCodePoint('?');
		}

		return len;
	}

	private static int utf8LengthAfterUnescape(BigMappedByteBuffer buffer, long start, long end) {
		int len = 0;
		long i = start;

		while (i < end) {
			byte b = buffer.get(i);
			if (b != '\\') {
				len++;
				i++;
				continue;
			}

			if (i + 1 >= end) {
				throw new IllegalArgumentException("Unescaped backslash in input buffer");
			}

			byte esc = buffer.get(i + 1);
			switch (esc) {
			case 't', 'r', 'n', '"', '\\' -> {
				len++;
				i += 2;
			}
			case 'u' -> {
				if (i + 5 >= end) {
					throw new IllegalArgumentException("Incomplete Unicode escape sequence in input buffer");
				}
				int cp = parseHex4(buffer, i + 2, end, 'u');
				len += utf8LengthCodePoint(cp);
				i += 6;
			}
			case 'U' -> {
				if (i + 9 >= end) {
					throw new IllegalArgumentException("Incomplete Unicode escape sequence in input buffer");
				}
				int cp = parseHex8ToInt(buffer, i + 2, end, 'U');
				len += utf8LengthCodePoint(cp);
				i += 10;
			}
			default -> throw new IllegalArgumentException("Unescaped backslash in input buffer");
			}
		}

		return len;
	}

	private static int utf8LengthEmitChar(char c, int pendingHigh, int len) {
		if (pendingHigh != -1) {
			len += utf8LengthCodePoint('?');
		}
		return len + utf8LengthEmitNonSurrogateChar(c);
	}

	private static int utf8LengthEmitNonSurrogateChar(char c) {
		if (Character.isHighSurrogate(c) || Character.isLowSurrogate(c)) {
			return utf8LengthCodePoint('?');
		}
		return utf8LengthCodePoint(c);
	}

	private static int writeUtf8AfterUnescape(BigMappedByteBuffer buffer, long start, long end, byte[] out, int off) {
		long i = start;
		while (i < end) {
			byte b = buffer.get(i);
			if (b != '\\') {
				out[off++] = b;
				i++;
				continue;
			}

			if (i + 1 >= end) {
				throw new IllegalArgumentException("Unescaped backslash in input buffer");
			}

			byte esc = buffer.get(i + 1);
			switch (esc) {
			case 't' -> {
				out[off++] = '\t';
				i += 2;
			}
			case 'r' -> {
				out[off++] = '\r';
				i += 2;
			}
			case 'n' -> {
				out[off++] = '\n';
				i += 2;
			}
			case '"' -> {
				out[off++] = '"';
				i += 2;
			}
			case '\\' -> {
				out[off++] = '\\';
				i += 2;
			}
			case 'u' -> {
				if (i + 5 >= end) {
					throw new IllegalArgumentException("Incomplete Unicode escape sequence in input buffer");
				}
				int cp = parseHex4(buffer, i + 2, end, 'u');
				off = writeUtf8CodePoint(cp, out, off);
				i += 6;
			}
			case 'U' -> {
				if (i + 9 >= end) {
					throw new IllegalArgumentException("Incomplete Unicode escape sequence in input buffer");
				}
				int cp = parseHex8ToInt(buffer, i + 2, end, 'U');
				off = writeUtf8CodePoint(cp, out, off);
				i += 10;
			}
			default -> throw new IllegalArgumentException("Unescaped backslash in input buffer");
			}
		}
		return off;
	}

	private static int writeUtf8AfterUnescape(String s, int start, int end, byte[] out, int off) {
		int pendingHigh = -1;
		int i = start;

		while (i < end) {
			char c = s.charAt(i);

			if (c != '\\') {
				if (pendingHigh != -1) {
					if (Character.isLowSurrogate(c)) {
						int cp = Character.toCodePoint((char) pendingHigh, c);
						off = writeUtf8CodePoint(cp, out, off);
						pendingHigh = -1;
						i++;
						continue;
					}
					off = writeUtf8CodePoint('?', out, off);
					pendingHigh = -1;
				}

				if (Character.isHighSurrogate(c)) {
					pendingHigh = c;
				} else if (Character.isLowSurrogate(c)) {
					off = writeUtf8CodePoint('?', out, off);
				} else {
					off = writeUtf8CodePoint(c, out, off);
				}
				i++;
				continue;
			}

			if (i + 1 >= end) {
				throw new IllegalArgumentException("Unescaped backslash in: " + s);
			}

			char esc = s.charAt(i + 1);
			switch (esc) {
			case 't' -> {
				if (pendingHigh != -1) {
					off = writeUtf8CodePoint('?', out, off);
					pendingHigh = -1;
				}
				off = writeUtf8CodePoint('\t', out, off);
				i += 2;
				continue;
			}
			case 'r' -> {
				if (pendingHigh != -1) {
					off = writeUtf8CodePoint('?', out, off);
					pendingHigh = -1;
				}
				off = writeUtf8CodePoint('\r', out, off);
				i += 2;
				continue;
			}
			case 'n' -> {
				if (pendingHigh != -1) {
					off = writeUtf8CodePoint('?', out, off);
					pendingHigh = -1;
				}
				off = writeUtf8CodePoint('\n', out, off);
				i += 2;
				continue;
			}
			case '"' -> {
				if (pendingHigh != -1) {
					off = writeUtf8CodePoint('?', out, off);
					pendingHigh = -1;
				}
				off = writeUtf8CodePoint('"', out, off);
				i += 2;
				continue;
			}
			case '\\' -> {
				if (pendingHigh != -1) {
					off = writeUtf8CodePoint('?', out, off);
					pendingHigh = -1;
				}
				off = writeUtf8CodePoint('\\', out, off);
				i += 2;
				continue;
			}
			case 'u' -> {
				if (i + 5 >= end) {
					throw new IllegalArgumentException("Incomplete Unicode escape sequence in: " + s);
				}
				int cp = parseHex4(s, i + 2, end, 'u');
				char decoded = (char) cp;

				if (pendingHigh != -1) {
					if (Character.isLowSurrogate(decoded)) {
						off = writeUtf8CodePoint(Character.toCodePoint((char) pendingHigh, decoded), out, off);
						pendingHigh = -1;
						i += 6;
						continue;
					} else {
						off = writeUtf8CodePoint('?', out, off);
						pendingHigh = -1;
					}
				}

				if (Character.isHighSurrogate(decoded)) {
					pendingHigh = decoded;
				} else if (Character.isLowSurrogate(decoded)) {
					off = writeUtf8CodePoint('?', out, off);
				} else {
					off = writeUtf8CodePoint(decoded, out, off);
				}

				i += 6;
				continue;
			}
			case 'U' -> {
				if (i + 9 >= end) {
					throw new IllegalArgumentException("Incomplete Unicode escape sequence in: " + s);
				}
				int cp = parseHex8ToInt(s, i + 2, end, 'U');
				char[] chars = Character.toChars(cp);
				for (char decoded : chars) {
					if (pendingHigh != -1) {
						if (Character.isLowSurrogate(decoded)) {
							off = writeUtf8CodePoint(Character.toCodePoint((char) pendingHigh, decoded), out, off);
							pendingHigh = -1;
							continue;
						}
						off = writeUtf8CodePoint('?', out, off);
						pendingHigh = -1;
					}

					if (Character.isHighSurrogate(decoded)) {
						pendingHigh = decoded;
					} else if (Character.isLowSurrogate(decoded)) {
						off = writeUtf8CodePoint('?', out, off);
					} else {
						off = writeUtf8CodePoint(decoded, out, off);
					}
				}
				i += 10;
				continue;
			}
			default -> throw new IllegalArgumentException("Unescaped backslash in: " + s);
			}
		}

		if (pendingHigh != -1) {
			off = writeUtf8CodePoint('?', out, off);
		}

		return off;
	}

	private static int writeUtf8AfterUnescape(char[] buffer, int start, int end, byte[] out, int off) {
		int pendingHigh = -1;
		int i = start;

		while (i < end) {
			char c = buffer[i];

			if (c != '\\') {
				if (pendingHigh != -1) {
					if (Character.isLowSurrogate(c)) {
						int cp = Character.toCodePoint((char) pendingHigh, c);
						off = writeUtf8CodePoint(cp, out, off);
						pendingHigh = -1;
						i++;
						continue;
					}
					off = writeUtf8CodePoint('?', out, off);
					pendingHigh = -1;
				}

				if (Character.isHighSurrogate(c)) {
					pendingHigh = c;
				} else if (Character.isLowSurrogate(c)) {
					off = writeUtf8CodePoint('?', out, off);
				} else {
					off = writeUtf8CodePoint(c, out, off);
				}
				i++;
				continue;
			}

			if (i + 1 >= end) {
				throw new IllegalArgumentException("Unescaped backslash in input buffer");
			}

			char esc = buffer[i + 1];
			switch (esc) {
			case 't' -> {
				if (pendingHigh != -1) {
					off = writeUtf8CodePoint('?', out, off);
					pendingHigh = -1;
				}
				off = writeUtf8CodePoint('\t', out, off);
				i += 2;
				continue;
			}
			case 'r' -> {
				if (pendingHigh != -1) {
					off = writeUtf8CodePoint('?', out, off);
					pendingHigh = -1;
				}
				off = writeUtf8CodePoint('\r', out, off);
				i += 2;
				continue;
			}
			case 'n' -> {
				if (pendingHigh != -1) {
					off = writeUtf8CodePoint('?', out, off);
					pendingHigh = -1;
				}
				off = writeUtf8CodePoint('\n', out, off);
				i += 2;
				continue;
			}
			case '"' -> {
				if (pendingHigh != -1) {
					off = writeUtf8CodePoint('?', out, off);
					pendingHigh = -1;
				}
				off = writeUtf8CodePoint('\"', out, off);
				i += 2;
				continue;
			}
			case '\\' -> {
				if (pendingHigh != -1) {
					off = writeUtf8CodePoint('?', out, off);
					pendingHigh = -1;
				}
				off = writeUtf8CodePoint('\\', out, off);
				i += 2;
				continue;
			}
			case 'u' -> {
				if (i + 5 >= end) {
					throw new IllegalArgumentException("Incomplete Unicode escape sequence in input buffer");
				}
				int cp = parseHex4(buffer, i + 2, end, 'u');
				char decoded = (char) cp;

				if (pendingHigh != -1) {
					if (Character.isLowSurrogate(decoded)) {
						off = writeUtf8CodePoint(Character.toCodePoint((char) pendingHigh, decoded), out, off);
						pendingHigh = -1;
						i += 6;
						continue;
					} else {
						off = writeUtf8CodePoint('?', out, off);
						pendingHigh = -1;
					}
				}

				if (Character.isHighSurrogate(decoded)) {
					pendingHigh = decoded;
				} else if (Character.isLowSurrogate(decoded)) {
					off = writeUtf8CodePoint('?', out, off);
				} else {
					off = writeUtf8CodePoint(decoded, out, off);
				}

				i += 6;
				continue;
			}
			case 'U' -> {
				if (i + 9 >= end) {
					throw new IllegalArgumentException("Incomplete Unicode escape sequence in input buffer");
				}
				int cp = parseHex8ToInt(buffer, i + 2, end, 'U');
				char[] chars = Character.toChars(cp);
				for (char decoded : chars) {
					if (pendingHigh != -1) {
						if (Character.isLowSurrogate(decoded)) {
							off = writeUtf8CodePoint(Character.toCodePoint((char) pendingHigh, decoded), out, off);
							pendingHigh = -1;
							continue;
						}
						off = writeUtf8CodePoint('?', out, off);
						pendingHigh = -1;
					}

					if (Character.isHighSurrogate(decoded)) {
						pendingHigh = decoded;
					} else if (Character.isLowSurrogate(decoded)) {
						off = writeUtf8CodePoint('?', out, off);
					} else {
						off = writeUtf8CodePoint(decoded, out, off);
					}
				}
				i += 10;
				continue;
			}
			default -> throw new IllegalArgumentException("Unescaped backslash in input buffer");
			}
		}

		if (pendingHigh != -1) {
			off = writeUtf8CodePoint('?', out, off);
		}

		return off;
	}

	private static int utf8LengthCodePoint(int cp) {
		if (cp < 0x80) {
			return 1;
		}
		if (cp < 0x800) {
			return 2;
		}
		if (cp < 0x10000) {
			return 3;
		}
		return 4;
	}

	private static int writeUtf8CodePoint(int cp, byte[] out, int off) {
		if (cp < 0x80) {
			out[off++] = (byte) cp;
			return off;
		}
		if (cp < 0x800) {
			out[off++] = (byte) (0xC0 | (cp >> 6));
			out[off++] = (byte) (0x80 | (cp & 0x3F));
			return off;
		}
		if (cp < 0x10000) {
			out[off++] = (byte) (0xE0 | (cp >> 12));
			out[off++] = (byte) (0x80 | ((cp >> 6) & 0x3F));
			out[off++] = (byte) (0x80 | (cp & 0x3F));
			return off;
		}
		out[off++] = (byte) (0xF0 | (cp >> 18));
		out[off++] = (byte) (0x80 | ((cp >> 12) & 0x3F));
		out[off++] = (byte) (0x80 | ((cp >> 6) & 0x3F));
		out[off++] = (byte) (0x80 | (cp & 0x3F));
		return off;
	}

	private static long indexOf(BigMappedByteBuffer buffer, long start, long end, byte value) {
		for (long i = start; i < end; i++) {
			if (buffer.get(i) == value) {
				return i;
			}
		}
		return -1L;
	}

	private static int indexOf(char[] buffer, int start, int end, char value) {
		for (int i = start; i < end; i++) {
			if (buffer[i] == value) {
				return i;
			}
		}
		return -1;
	}

	private static int hexValue(byte b) {
		if (b >= '0' && b <= '9') {
			return b - '0';
		}
		if (b >= 'A' && b <= 'F') {
			return 10 + (b - 'A');
		}
		if (b >= 'a' && b <= 'f') {
			return 10 + (b - 'a');
		}
		return -1;
	}

	private static int hexValue(char c) {
		if (c >= '0' && c <= '9') {
			return c - '0';
		}
		if (c >= 'A' && c <= 'F') {
			return 10 + (c - 'A');
		}
		if (c >= 'a' && c <= 'f') {
			return 10 + (c - 'a');
		}
		return -1;
	}

	private static int parseHex4(BigMappedByteBuffer buffer, long pos, long end, char escapeType) {
		int h0 = hexValue(buffer.get(pos));
		int h1 = hexValue(buffer.get(pos + 1));
		int h2 = hexValue(buffer.get(pos + 2));
		int h3 = hexValue(buffer.get(pos + 3));
		if ((h0 | h1 | h2 | h3) < 0) {
			String xx = new String(new char[] { (char) buffer.get(pos), (char) buffer.get(pos + 1),
					(char) buffer.get(pos + 2), (char) buffer.get(pos + 3) });
			throw new IllegalArgumentException(
					"Illegal Unicode escape sequence '\\" + escapeType + xx + "' in input buffer");
		}
		return (h0 << 12) | (h1 << 8) | (h2 << 4) | h3;
	}

	private static int parseHex4(char[] buffer, int pos, int end, char escapeType) {
		int h0 = hexValue(buffer[pos]);
		int h1 = hexValue(buffer[pos + 1]);
		int h2 = hexValue(buffer[pos + 2]);
		int h3 = hexValue(buffer[pos + 3]);
		if ((h0 | h1 | h2 | h3) < 0) {
			String xx = new String(buffer, pos, 4);
			throw new IllegalArgumentException(
					"Illegal Unicode escape sequence '\\" + escapeType + xx + "' in input buffer");
		}
		return (h0 << 12) | (h1 << 8) | (h2 << 4) | h3;
	}

	private static int parseHex4(String s, int pos, int end, char escapeType) {
		int h0 = hexValue(s.charAt(pos));
		int h1 = hexValue(s.charAt(pos + 1));
		int h2 = hexValue(s.charAt(pos + 2));
		int h3 = hexValue(s.charAt(pos + 3));
		if ((h0 | h1 | h2 | h3) < 0) {
			String xx = s.substring(pos, pos + 4);
			throw new IllegalArgumentException("Illegal Unicode escape sequence '\\" + escapeType + xx + "' in: " + s);
		}
		return (h0 << 12) | (h1 << 8) | (h2 << 4) | h3;
	}

	private static int parseHex8ToInt(BigMappedByteBuffer buffer, long pos, long end, char escapeType) {
		long value = 0;
		for (int i = 0; i < 8; i++) {
			int hv = hexValue(buffer.get(pos + i));
			if (hv < 0) {
				StringBuilder sb = new StringBuilder(8);
				for (int j = 0; j < 8; j++) {
					sb.append((char) buffer.get(pos + j));
				}
				throw new IllegalArgumentException(
						"Illegal Unicode escape sequence '\\" + escapeType + sb + "' in input buffer");
			}
			value = (value << 4) | hv;
		}
		if (value > Integer.MAX_VALUE) {
			StringBuilder sb = new StringBuilder(8);
			for (int j = 0; j < 8; j++) {
				sb.append((char) buffer.get(pos + j));
			}
			throw new IllegalArgumentException(
					"Illegal Unicode escape sequence '\\" + escapeType + sb + "' in input buffer");
		}
		return (int) value;
	}

	private static int parseHex8ToInt(char[] buffer, int pos, int end, char escapeType) {
		long value = 0;
		for (int i = 0; i < 8; i++) {
			int hv = hexValue(buffer[pos + i]);
			if (hv < 0) {
				String xx = new String(buffer, pos, 8);
				throw new IllegalArgumentException(
						"Illegal Unicode escape sequence '\\" + escapeType + xx + "' in input buffer");
			}
			value = (value << 4) | hv;
		}
		if (value > Integer.MAX_VALUE) {
			String xx = new String(buffer, pos, 8);
			throw new IllegalArgumentException(
					"Illegal Unicode escape sequence '\\" + escapeType + xx + "' in input buffer");
		}
		return (int) value;
	}

	private static int parseHex8ToInt(String s, int pos, int end, char escapeType) {
		long value = 0;
		for (int i = 0; i < 8; i++) {
			int hv = hexValue(s.charAt(pos + i));
			if (hv < 0) {
				String xx = s.substring(pos, pos + 8);
				throw new IllegalArgumentException(
						"Illegal Unicode escape sequence '\\" + escapeType + xx + "' in: " + s);
			}
			value = (value << 4) | hv;
		}
		if (value > Integer.MAX_VALUE) {
			String xx = s.substring(pos, pos + 8);
			throw new IllegalArgumentException("Illegal Unicode escape sequence '\\" + escapeType + xx + "' in: " + s);
		}
		return (int) value;
	}

	/**
	 * Converts a decimal value to a hexadecimal string representation of the
	 * specified length.
	 *
	 * @param decimal      A decimal value.
	 * @param stringLength The length of the resulting string.
	 * @return String
	 */
	public static String toHexString(int decimal, int stringLength) {
		StringBuilder sb = new StringBuilder(stringLength);

		String hexVal = Integer.toHexString(decimal).toUpperCase();

		// insert zeros if hexVal has less than stringLength characters:
		int nofZeros = stringLength - hexVal.length();
		for (int i = 0; i < nofZeros; i++) {
			sb.append('0');
		}

		sb.append(hexVal);

		return sb.toString();
	}
}
