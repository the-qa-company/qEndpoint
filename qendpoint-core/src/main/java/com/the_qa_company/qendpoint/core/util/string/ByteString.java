package com.the_qa_company.qendpoint.core.util.string;

import java.util.Arrays;

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
	 * copy a CharSequence into a new byte string
	 *
	 * @param csq char sequence
	 * @return byte string
	 */
	static ByteString copy(CharSequence csq) {
		if (csq instanceof ByteString bs) {
			return bs.copy();
		}
		return new CompactString(csq);
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
		if (this == other) {
			return 0;

		}

		// Cache lengths once (avoid repeated virtual calls in hot loops)
		final int len1 = this.length();
		final int len2 = other.length();
		final int n = Math.min(len1, len2);

		// Grab backing arrays once
		final byte[] a = this.getBuffer();
		final byte[] b = other.getBuffer();

		// HotSpot intrinsic on Java 9+: vectorized/word-at-a-time mismatch
		// search
		final int mismatch = Arrays.mismatch(a, 0, n, b, 0, n);
		if (mismatch >= 0) {
			// IMPORTANT: unsigned compare to match your charAt() semantics
			// (0..255)
			return (a[mismatch] & 0xFF) - (b[mismatch] & 0xFF);
		}

		// All equal up to min length => shorter one is "smaller"
		return len1 - len2;
	}

	default int compareTo(CharSequence other) {
		// Fast path when the "CharSequence" is actually a ByteString
		if (other instanceof ByteString bs) {
			return compareTo(bs);
		}

		// Fallback: generic CharSequence compare
		final int len1 = length();
		final int len2 = other.length();
		final int n = Math.min(len1, len2);

		for (int i = 0; i < n; i++) {
			final char c1 = charAt(i);
			final char c2 = other.charAt(i);
			if (c1 != c2) {
				return c1 - c2;
			}
		}
		return len1 - len2;
	}

	@Override
	ByteString subSequence(int start, int end);

	default ByteString subSequence(int start) {
		return subSequence(start, length());
	}

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
		if (other.isEmpty()) {
			return this;
		}
		if (isEmpty()) {
			return other;
		}
		byte[] buffer = new byte[length() + other.length()];
		// prefix
		System.arraycopy(getBuffer(), 0, buffer, 0, length());
		// text
		System.arraycopy(other.getBuffer(), 0, buffer, length(), other.length());
		return new CompactString(buffer);
	}

	default ByteString copyAppend(ByteString other, int start) {
		return copyAppend(other, start, other.length() - start);
	}

	default ByteString copyAppend(ByteString other, int start, int len) {
		if (len == 0) {
			return this;
		}
		if (isEmpty()) {
			if (len == other.length()) {
				return other;
			}
			return other.subSequence(start, len);
		}
		byte[] buffer = new byte[length() + len];
		// prefix
		System.arraycopy(getBuffer(), 0, buffer, 0, length());
		// text
		System.arraycopy(other.getBuffer(), start, buffer, length(), len);
		return new CompactString(buffer);
	}

	/**
	 * copy this string and append another string
	 *
	 * @param other other string
	 * @return new byte string
	 */
	default ByteString copyPreAppend(CharSequence other) {
		return ByteString.of(other).copyAppend(this);
	}

	/**
	 * copy this string and append another string
	 *
	 * @param other other string
	 * @return new byte string
	 */
	default ByteString copyPreAppend(ByteString other) {
		return other.copyAppend(this);
	}

	/**
	 * @return copy this byte string into another one
	 */
	default ByteString copy() {
		return new CompactString(this);
	}

	@Override
	boolean equals(Object other);

	/**
	 * test if this ByteString starts with another one
	 *
	 * @param prefix prefix
	 * @return true if this string starts with prefix
	 */
	default boolean startsWith(ByteString prefix) {
		return startsWith(prefix, 0);
	}

	/**
	 * test if this ByteString starts with another one
	 *
	 * @param prefix prefix
	 * @param start  start location in this string
	 * @return true if this string starts with prefix
	 */
	default boolean startsWith(ByteString prefix, int start) {
		if (start + length() < prefix.length()) {
			return false; // too long
		}

		for (int i = 0; i < prefix.length(); i++) {
			if (charAt(i + start) != prefix.charAt(i)) {
				return false;
			}
		}
		return true;
	}
}
