package com.the_qa_company.qendpoint.core.util;

import com.the_qa_company.qendpoint.core.util.string.ByteString;

/**
 * Utilities to hash strings based on the <a href=
 * "https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function">FNV1A
 * algorithm</a>
 *
 * @author Antoine Willerval
 */
public class HashUtil {
	public static final long MASK39 = 0x7FFFFFFFFFL;
	public static final long MASK32 = 0xFFFFFFFFL;
	public static final long FNV1A_64_OFFSET = 0xcbf29ce484222325L;
	public static final long FNV1A_64_IV = 0x00000100000001b3L;
	public static final long FNV1A_32_OFFSET = 0x811c9dc5L;
	public static final long FNV1A_32_IV = 0x01000193;

	public static long hashFNV64(ByteString str) {
		int len = str.length();

		if (len == 0)
			return 0;

		long hash = FNV1A_64_OFFSET;

		for (int i = 0; i < len; i++) {
			hash = (hash ^ str.charAt(i)) * FNV1A_64_IV;
		}

		return hash;
	}

	public static long hashFNV32(ByteString str) {
		int len = str.length();

		if (len == 0)
			return 0;

		long hash = FNV1A_32_OFFSET;

		for (int i = 0; i < len; i++) {
			hash = (hash ^ str.charAt(i)) * FNV1A_32_IV;
		}

		return hash & MASK32;
	}

	public static long hashFNV39(ByteString str) {
		long fnv1a64 = hashFNV64(str);

		return (fnv1a64 >>> 39) ^ (fnv1a64 & MASK39);
	}

}
