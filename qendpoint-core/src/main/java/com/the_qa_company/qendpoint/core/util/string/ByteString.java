package com.the_qa_company.qendpoint.core.util.string;

/**
 * ByteString char sequence, can't be compared with string, faster than string
 * with IO
 */
public interface ByteString extends CharSequence, Comparable<ByteString> {
	/**
	 * @return empty byte string
	 */
	static ByteString empty() {
		return CompactString.EMPTY;
	}

	/**
	 * convert (if required) to a ByteString, this method might not copy the
	 * ByteString
	 *
	 * @param sec char sequence
	 * @return byte string
	 */
	static ByteString of(CharSequence sec) {
		return ByteStringUtil.asByteString(sec);
	}

	/**
	 * @return the buffer associated with this byte string, the maximum size
	 *         should be read with {@link #length()}
	 */
	byte[] getBuffer();

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	default int compareTo(ByteString other) {
		int n = Math.min(length(), other.length());
		int k = 0;
		while (k < n) {
			char c1 = charAt(k);
			char c2 = other.charAt(k);
			if (c1 != c2) {
				return c1 - c2;
			}
			k++;
		}
		return length() - other.length();
	}

	@Override
	ByteString subSequence(int start, int end);

	/**
	 * copy this string and append another string
	 *
	 * @param other other string
	 * @return new byte string
	 */
	default ByteString copyAppend(CharSequence other) {
		return copyAppend(ByteString.of(other));
	}

	/**
	 * copy this string and append another string
	 *
	 * @param other other string
	 * @return new byte string
	 */
	default ByteString copyAppend(ByteString other) {
		byte[] buffer = new byte[length() + other.length()];
		// prefix
		System.arraycopy(getBuffer(), 0, buffer, 0, length());
		// text
		System.arraycopy(other.getBuffer(), 0, buffer, length(), other.length());
		return new CompactString(buffer);
	}

	/**
	 * @return copy this byte string into another one
	 */
	default ByteString copy() {
		return new CompactString(this);
	}
}
