package com.the_qa_company.qendpoint.core.util.string;

import java.util.Arrays;

public final class ByteStringInterner {
	private static final int MIN_CAPACITY = 16;
	private static final float LOAD_FACTOR = 0.7f;

	private final int maxCapacity;
	private CompactString[] table;
	private int size;
	private int resizeThreshold;
	private int mask;
	private byte[] scratch = new byte[64];

	public ByteStringInterner(int expectedEntries) {
		this(expectedEntries, expectedEntries);
	}

	public ByteStringInterner(int expectedEntries, int maxEntries) {
		int target = Math.max(MIN_CAPACITY, (int) Math.ceil(expectedEntries / LOAD_FACTOR));
		int capacity = tableSizeFor(target);
		int maxCap = Math.max(capacity, tableSizeFor(maxEntries));
		this.table = new CompactString[capacity];
		this.mask = capacity - 1;
		this.resizeThreshold = Math.max(1, (int) (capacity * LOAD_FACTOR));
		this.maxCapacity = maxCap;
	}

	public void ensureCapacity(int expectedEntries) {
		if (expectedEntries <= 0) {
			return;
		}
		int target = Math.max(MIN_CAPACITY, (int) Math.ceil(expectedEntries / LOAD_FACTOR));
		int desired = tableSizeFor(target);
		if (desired <= table.length) {
			return;
		}
		int newCapacity = Math.min(maxCapacity, desired);
		if (newCapacity > table.length) {
			rehashTo(newCapacity);
		}
	}

	public byte[] ensureScratchCapacity(int required) {
		if (required <= scratch.length) {
			return scratch;
		}
		scratch = new byte[tableSizeFor(required)];
		return scratch;
	}

	public ByteString internScratch(byte[] buffer, int len) {
		if (len <= 0) {
			return ByteString.empty();
		}
		return internInternal(buffer, len);
	}

	public int size() {
		return size;
	}

	public void clear() {
		Arrays.fill(table, null);
		size = 0;
	}

	private ByteString internInternal(byte[] buffer, int len) {
		int hash = hash(buffer, len);
		if (size + 1 >= resizeThreshold && table.length >= maxCapacity) {
			clear();
		}
		int idx = hash & mask;

		while (true) {
			CompactString existing = table[idx];
			if (existing == null) {
				CompactString created = new CompactString(Arrays.copyOf(buffer, len));
				table[idx] = created;
				size++;
				if (size >= resizeThreshold && table.length < maxCapacity) {
					rehash();
				}
				return created;
			}
			if (equals(existing.data, buffer, len)) {
				return existing;
			}
			idx = (idx + 1) & mask;
		}
	}

	private void rehash() {
		CompactString[] oldTable = table;
		int newCapacity = Math.min(maxCapacity, oldTable.length << 1);
		rehashTo(newCapacity);
	}

	private void rehashTo(int newCapacity) {
		CompactString[] oldTable = table;
		CompactString[] newTable = new CompactString[newCapacity];
		int newMask = newCapacity - 1;

		for (CompactString entry : oldTable) {
			if (entry == null) {
				continue;
			}
			int idx = hash(entry.data, entry.data.length) & newMask;
			while (newTable[idx] != null) {
				idx = (idx + 1) & newMask;
			}
			newTable[idx] = entry;
		}

		this.table = newTable;
		this.mask = newMask;
		this.resizeThreshold = (int) (newCapacity * LOAD_FACTOR);
	}

	private static boolean equals(byte[] data, byte[] buffer, int len) {
		if (data.length != len) {
			return false;
		}
		for (int i = 0; i < len; i++) {
			if (data[i] != buffer[i]) {
				return false;
			}
		}
		return true;
	}

	private static int hash(byte[] buffer, int len) {
		int hash = (int) 2166136261L;
		for (int i = 0; i < len; i++) {
			hash = (hash * 16777619) ^ buffer[i];
		}
		return hash;
	}

	private static int tableSizeFor(int cap) {
		int n = 1;
		while (n < cap) {
			n <<= 1;
		}
		return n;
	}
}
