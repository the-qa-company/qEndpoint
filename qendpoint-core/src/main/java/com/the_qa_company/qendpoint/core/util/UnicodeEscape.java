package com.the_qa_company.qendpoint.core.util;

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
