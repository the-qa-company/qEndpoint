package com.the_qa_company.qendpoint.core.util.io;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.BaseStream;

/**
 * Class to close many {@link java.io.Closeable} objects at once without having
 * to do a large try-finally tree, handle {@link Closeable}, {@link Iterable},
 * array, record, {@link Map}, {@link Stream}, the {@link Throwable} are also
 * rethrown, it can be useful to close and throw at the same time.
 * <p>
 * It's using a deep search over the elements.
 *
 * @author Antoine Willerval
 */
public class Closer implements Iterable<Closeable>, Closeable {

	// Cache record component accessors per record class.
	// ClassValue is GC-friendly and avoids classloader leaks.
	private static final ClassValue<Method[]> RECORD_ACCESSORS = new ClassValue<>() {
		@Override
		protected Method[] computeValue(Class<?> type) {
			if (!type.isRecord()) {
				return new Method[0];
			}
			RecordComponent[] components = type.getRecordComponents();
			Method[] accessors = new Method[components.length];
			for (int i = 0; i < components.length; i++) {
				Method m = components[i].getAccessor();
				// Avoid throwing InaccessibleObjectException in modular
				// environments.
				// If we can't make it accessible, invoke() may still work for
				// public accessors.
				try {
					m.trySetAccessible();
				} catch (Throwable ignored) {
					// Best-effort only.
				}
				accessors[i] = m;
			}
			return accessors;
		}
	};

	private final List<Closeable> list;
	private final IdentityHashMap<Object, Boolean> seen; // cycle/dedup guard

	private Closer(Object... other) {
		this.list = (other == null) ? new ArrayList<>() : new ArrayList<>(other.length);
		this.seen = new IdentityHashMap<>();
		if (other != null) {
			for (Object o : other) {
				addDeep(o);
			}
		}
	}

	public static Closer of(Object... other) {
		return new Closer(other);
	}

	public static void closeAll(Object... other) throws IOException {
		Closer c = new Closer(other);
		c.close();
	}

	public static void closeSingle(Object other) throws IOException {
		closeAll(other);
	}

	public Closer with(Object other, Object... otherList) {
		addDeep(other);
		if (otherList != null) {
			for (Object o : otherList) {
				addDeep(o);
			}
		}
		return this;
	}

	/**
	 * Iterative DFS over object graph, registering close actions. Never
	 * intentionally throws (except catastrophic JVM errors), so we don't leak
	 * already-discovered resources during traversal.
	 */
	private void addDeep(Object root) {
		if (root == null) {
			return;
		}

		Deque<Object> stack = new ArrayDeque<>();
		pushIfNotNull(stack, root);

		while (!stack.isEmpty()) {
			Object obj = stack.pop();
			if (obj == null) {
				continue;
			}

			// Cycle/dedup guard:
			// we only process a specific object identity once.
			if (seen.put(obj, Boolean.TRUE) != null) {
				continue;
			}

			// 1) Closeable leaf
			if (obj instanceof Closeable c) {
				list.add(c);
				continue;
			}

			// 2) Stream/BaseStream: close it AND traverse its elements
			// (consumes it!)
			// Must be checked BEFORE AutoCloseable, since BaseStream is
			// AutoCloseable.
			if (obj instanceof BaseStream<?, ?> bs) {
				// Register closing of the stream itself.
				list.add(bs::close);

				// Traverse elements (this will consume the stream).
				try {
					Iterator<?> it = bs.iterator();
					while (it.hasNext()) {
						pushIfNotNull(stack, it.next());
					}
				} catch (Throwable t) {
					// Record traversal failure as "high value" throwable.
					list.add(throwingHighValue(t));
				}
				continue;
			}

			// 3) AutoCloseable (non-Closeable): wrap it so we can store
			// Closeable
			if (obj instanceof AutoCloseable ac) {
				list.add(wrapAutoCloseable(ac));
				continue;
			}

			// 4) Iterable container
			if (obj instanceof Iterable<?> it) {
				try {
					for (Object e : it) {
						pushIfNotNull(stack, e);
					}
				} catch (Throwable t) {
					list.add(throwingHighValue(t));
				}
				continue;
			}

			// 5) Object[] container (ignore primitive arrays)
			if (obj instanceof Object[] arr) {
				// Push in reverse so traversal order is closer to natural array
				// order
				// for a LIFO stack.
				for (int i = arr.length - 1; i >= 0; i--) {
					pushIfNotNull(stack, arr[i]);
				}
				continue;
			}

			// 6) Map container: traverse keys + values
			if (obj instanceof Map<?, ?> map) {
				try {
					for (Object v : map.values()) {
						pushIfNotNull(stack, v);
					}
					for (Object k : map.keySet()) {
						pushIfNotNull(stack, k);
					}
				} catch (Throwable t) {
					list.add(throwingHighValue(t));
				}
				continue;
			}

			// 7) Record: traverse components reflectively
			Class<?> cls = obj.getClass();
			if (cls.isRecord()) {
				Method[] accessors = RECORD_ACCESSORS.get(cls);
				for (int i = accessors.length - 1; i >= 0; i--) {
					Method m = accessors[i];
					try {
						Object value = m.invoke(obj);
						pushIfNotNull(stack, value);
					} catch (IllegalAccessException | InvocationTargetException e) {
						list.add(throwingHighValue(new IOException(
								"Can't read record component via " + cls.getName() + "#" + m.getName(), e)));
					} catch (Throwable t) {
						list.add(throwingHighValue(t));
					}
				}
				continue;
			}

			// 8) Throwable: "close and throw" feature
			if (obj instanceof Throwable t) {
				list.add(throwingHighValue(t));
				continue;
			}

			// Unknown type: ignore
		}
	}

	private static void pushIfNotNull(Deque<Object> stack, Object value) {
		if (value != null) {
			stack.push(value);
		}
	}

	@Override
	public Iterator<Closeable> iterator() {
		return list.iterator();
	}

	@Override
	public void close() throws IOException {
		if (list.isEmpty()) {
			return;
		}

		// Close in reverse order (try-with-resources semantics).
		// JLS: resources are closed in reverse order of initialization.
		// [oai_citation:8‡Oracle
		// Documentation](https://docs.oracle.com/javase/specs/jls/se8/html/jls-14.html)
		List<Throwable> failures = null;

		for (int i = list.size() - 1; i >= 0; i--) {
			Closeable c = list.get(i);
			if (c == null) {
				continue;
			}

			try {
				c.close();
			} catch (Throwable t) {
				if (failures == null) {
					failures = new ArrayList<>();
				}
				failures.add(t);
			}
		}

		if (failures == null) {
			return;
		}

		// Select primary failure by "severity":
		// Error > HighValueException > RuntimeException > checked/other.
		Throwable primary = failures.get(0);
		int primarySeverity = severity(primary);

		for (int i = 1; i < failures.size(); i++) {
			Throwable t = failures.get(i);
			int sev = severity(t);
			if (sev > primarySeverity) {
				primary = t;
				primarySeverity = sev;
			}
		}

		Throwable primaryToThrow = unwrapHighValue(primary);

		for (Throwable t : failures) {
			if (t == primary) {
				continue;
			}
			Throwable suppressed = unwrapHighValue(t);
			if (suppressed == primaryToThrow) {
				continue;
			}
			primaryToThrow.addSuppressed(suppressed);
		}

		throwIOOrRuntime(primaryToThrow);
	}

	private static int severity(Throwable t) {
		if (t instanceof Error) {
			return 3;
		}
		if (t instanceof HighValueException) {
			return 2;
		}
		if (t instanceof RuntimeException) {
			return 1;
		}
		return 0;
	}

	private static Throwable unwrapHighValue(Throwable t) {
		if (t instanceof HighValueException hve && hve.getCause() != null) {
			return hve.getCause();
		}
		return t;
	}

	private static Closeable wrapAutoCloseable(AutoCloseable ac) {
		return () -> {
			try {
				ac.close();
			} catch (IOException ioe) {
				throw ioe;
			} catch (RuntimeException | Error e) {
				throw e;
			} catch (Exception e) {
				throw new IOException(e);
			}
		};
	}

	/**
	 * Returns a Closeable that throws the given throwable when closed, but
	 * wrapped as HighValueException (unless it's an Error) so it wins against
	 * ordinary close failures.
	 */
	private static Closeable throwingHighValue(Throwable t) {
		return () -> {
			if (t instanceof Error err) {
				throw err;
			}
			if (t instanceof RuntimeException re) {
				throw new HighValueException(re);
			}
			if (t instanceof IOException ioe) {
				throw new HighValueException(ioe);
			}
			throw new HighValueException(new IOException(t));
		};
	}

	private static void throwIOOrRuntime(Throwable t) throws IOException {
		if (t instanceof IOException ioe) {
			throw ioe;
		}
		if (t instanceof RuntimeException re) {
			throw re;
		}
		if (t instanceof Error err) {
			throw err;
		}
		throw new IOException(t);
	}

	private static class HighValueException extends RuntimeException {
		HighValueException(Throwable cause) {
			super(cause);
		}
	}
}
