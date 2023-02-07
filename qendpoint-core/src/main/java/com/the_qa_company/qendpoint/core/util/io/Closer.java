package com.the_qa_company.qendpoint.core.util.io;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Class to close many {@link java.io.Closeable} objects at once without having
 * to do a large try-finally tree
 *
 * @author Antoine Willerval
 */
public class Closer implements Iterable<Closeable>, Closeable {
	private final List<Closeable> list;

	private Closer(Object... other) {
		list = new ArrayList<>(other.length);
		with(other);
	}

	/**
	 * create closer with closeables
	 *
	 * @param other closeables
	 * @return closer
	 */
	public static Closer of(Object... other) {
		return new Closer(other);
	}

	/**
	 * close all the whatever closeable contained by these objects, easier and
	 * faster to write than a large try-finally tree
	 *
	 * @param other objects to close
	 * @throws IOException close exception
	 */
	public static void closeAll(Object... other) throws IOException {
		of(other).close();
	}

	/**
	 * add closeables to this closer
	 *
	 * @param other closeables
	 * @return this
	 */
	public Closer with(Object other, Object... otherList) {
		Stream.concat(Stream.of(other), Arrays.stream(otherList)).flatMap(this::explore).forEach(list::add);
		return this;
	}

	private Stream<Closeable> explore(Object obj) {
		// already a closeable, no need to map
		if (obj instanceof Closeable) {
			return Stream.of((Closeable) obj);
		}

		// collection object, we need to map all the elements
		if (obj instanceof Iterable) {
			return StreamSupport.stream(((Iterable<?>) obj).spliterator(), false).flatMap(this::explore);
		}

		// array object
		if (obj instanceof Object[]) {
			return Stream.of((Object[]) obj).flatMap(this::explore);
		}

		// map object, we need to map all the key+values
		if (obj instanceof Map) {
			Map<?, ?> map = (Map<?, ?>) obj;
			return Stream.concat(explore((map).keySet()), explore((map).values()));
		}

		// nothing known
		return Stream.of();
	}

	@Override
	public Iterator<Closeable> iterator() {
		return list.iterator();
	}

	@Override
	public void close() throws IOException {
		IOUtil.closeAll(list);
	}
}
