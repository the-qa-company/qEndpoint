package com.the_qa_company.qendpoint.core.triples.impl;

import java.util.Arrays;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.StampedLock;

final class LongArrayPool {
	private static final int ARRAY_HEADER_BYTES = 16;
	private static final String ENABLED_PROPERTY = "qendpoint.longArrayPool.enabled";
	private static final String CONCURRENCY_PROPERTY = "qendpoint.longArrayPool.concurrency";
	private static final int MIN_POOLED_LENGTH = 4;
	private static final int MAX_RETAINED_STACK_LENGTH = 16;
	private static final int DEFAULT_CONCURRENCY = Math.max(1, Runtime.getRuntime().availableProcessors());
	private static final long MAX_POOL_BYTES = Math.max(0L, Runtime.getRuntime().maxMemory() / 5L);
	private static volatile boolean enabled = Boolean.parseBoolean(System.getProperty(ENABLED_PROPERTY, "true"));
	private static volatile PoolGroup poolGroup = new PoolGroup(readConcurrency());

	private LongArrayPool() {
	}

	static void setEnabled(boolean enabled) {
		if (LongArrayPool.enabled == enabled) {
			return;
		}
		LongArrayPool.enabled = enabled;
		if (!enabled) {
			clearPool();
		}
	}

	static void setConcurrency(int concurrency) {
		int normalized = normalizeConcurrency(concurrency);
		PoolGroup current = poolGroup;
		if (current.concurrency == normalized) {
			return;
		}
		poolGroup = new PoolGroup(normalized);
	}

	static long[] borrow(int minLength) {
		if (minLength <= 0) {
			return new long[0];
		}
		int length = Math.max(1, minLength);
		if (!enabled || length < MIN_POOLED_LENGTH) {
			return new long[length];
		}
		return poolGroup.poolForThread().borrow(length);
	}

	static void release(long[] array) {
		if (!enabled) {
			return;
		}
		if (array == null || array.length == 0) {
			return;
		}
		if (array.length < MIN_POOLED_LENGTH) {
			return;
		}
		poolGroup.poolForThread().release(array);
	}

	private static long estimateBytes(int length) {
		return ARRAY_HEADER_BYTES + (long) length * Long.BYTES;
	}

	private static void clearPool() {
		poolGroup.clear();
	}

	private static int readConcurrency() {
		String raw = System.getProperty(CONCURRENCY_PROPERTY);
		if (raw == null) {
			return DEFAULT_CONCURRENCY;
		}
		try {
			return normalizeConcurrency(Integer.parseInt(raw.trim()));
		} catch (NumberFormatException ignored) {
			return DEFAULT_CONCURRENCY;
		}
	}

	private static int normalizeConcurrency(int concurrency) {
		return Math.max(1, concurrency);
	}

	private static final class PoolGroup {
		private final SubPool[] pools;
		private final int concurrency;

		private PoolGroup(int concurrency) {
			this.concurrency = concurrency;
			long maxPoolBytes = maxPoolBytesPerPool(concurrency);
			this.pools = new SubPool[concurrency];
			for (int i = 0; i < concurrency; i++) {
				pools[i] = new SubPool(maxPoolBytes);
			}
		}

		private SubPool poolForThread() {
			int index = Math.floorMod(Thread.currentThread().getId(), concurrency);
			return pools[index];
		}

		private void clear() {
			for (SubPool pool : pools) {
				pool.clear();
			}
		}
	}

	private static long maxPoolBytesPerPool(int concurrency) {
		if (MAX_POOL_BYTES <= 0L) {
			return 0L;
		}
		return Math.max(0L, MAX_POOL_BYTES / concurrency);
	}

	private static boolean shouldRetainStack(int length) {
		return length >= MIN_POOLED_LENGTH && length <= MAX_RETAINED_STACK_LENGTH;
	}

	private static final class SubPool {
		private final StampedLock lock = new StampedLock();
		private final NavigableMap<Integer, LongArrayStack> pool = new TreeMap<>();
		private final long maxPoolBytes;
		private long pooledBytes = 0L;

		private SubPool(long maxPoolBytes) {
			this.maxPoolBytes = maxPoolBytes;
		}

		private long[] borrow(int length) {
			long stamp = lock.readLock();
			boolean hasCandidate = false;
			try {
				var entry = pool.ceilingEntry(length);
				while (entry != null && entry.getValue().isEmpty()) {
					entry = pool.higherEntry(entry.getKey());
				}
				if (entry != null) {
					hasCandidate = true;
				}
			} finally {
				lock.unlockRead(stamp);
			}
			if (!hasCandidate) {
				return new long[length];
			}

			long writeStamp = lock.writeLock();
			try {
				var entry = pool.ceilingEntry(length);
				while (entry != null) {
					LongArrayStack stack = entry.getValue();
					if (!stack.isEmpty()) {
						long[] array = stack.pop();
						if (stack.isEmpty() && !shouldRetainStack(entry.getKey())) {
							pool.remove(entry.getKey());
						}
						if (array != null) {
							pooledBytes -= estimateBytes(array.length);
							return array;
						}
					} else if (!shouldRetainStack(entry.getKey())) {
						pool.remove(entry.getKey());
					}
					entry = pool.higherEntry(entry.getKey());
				}
			} finally {
				lock.unlockWrite(writeStamp);
			}
			return new long[length];
		}

		private void release(long[] array) {
			long bytes = estimateBytes(array.length);
			if (bytes <= 0L || maxPoolBytes <= 0L || bytes > maxPoolBytes) {
				return;
			}
			long stamp = lock.writeLock();
			try {
				while (pooledBytes + bytes > maxPoolBytes) {
					var entry = pool.lastEntry();
					while (entry != null && entry.getValue().isEmpty()) {
						if (!shouldRetainStack(entry.getKey())) {
							pool.remove(entry.getKey());
						}
						entry = pool.lowerEntry(entry.getKey());
					}
					if (entry == null) {
						break;
					}
					LongArrayStack stack = entry.getValue();
					long[] removed = stack.pop();
					if (removed != null) {
						pooledBytes -= estimateBytes(removed.length);
					}
					if (stack.isEmpty() && !shouldRetainStack(entry.getKey())) {
						pool.remove(entry.getKey());
					}
				}
				if (pooledBytes + bytes > maxPoolBytes) {
					return;
				}
				pool.computeIfAbsent(array.length, k -> new LongArrayStack()).push(array);
				pooledBytes += bytes;
			} finally {
				lock.unlockWrite(stamp);
			}
		}

		private void clear() {
			long stamp = lock.writeLock();
			try {
				pool.clear();
				pooledBytes = 0L;
			} finally {
				lock.unlockWrite(stamp);
			}
		}
	}

	private static final class LongArrayStack {
		private long[][] elements = new long[1][];
		private int size = 0;

		private boolean isEmpty() {
			return size == 0;
		}

		private void push(long[] array) {
			if (size == elements.length) {
				elements = Arrays.copyOf(elements, size * 2);
			}
			elements[size++] = array;
		}

		private long[] pop() {
			if (size == 0) {
				return null;
			}
			int index = --size;
			long[] array = elements[index];
			elements[index] = null;
			return array;
		}
	}
}
