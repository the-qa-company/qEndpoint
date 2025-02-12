package com.the_qa_company.qendpoint.core.util.string;

import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

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

//	/*
//	 * (non-Javadoc)
//	 * @see java.lang.Comparable#compareTo(java.lang.Object)
//	 */
//	@Override
//	default int compareTo(ByteString other) {
//		int n = Math.min(length(), other.length());
//
//		if (n == 0) {
//			return length() - other.length();
//		}
//
//		byte[] buffer = getBuffer();
//		byte[] buffer1 = other.getBuffer();
//
//		int mismatch = Arrays.mismatch(buffer, buffer1);
//		if (mismatch == -1) {
//			return length() - other.length();
//		}
//
//		int k = mismatch;
//		while (k < n) {
//			char c1 = charAt(k);
//			char c2 = other.charAt(k);
//			if (c1 != c2) {
//				return c1 - c2;
//			}
//			k++;
//		}
//		return length() - other.length();
//	}

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
