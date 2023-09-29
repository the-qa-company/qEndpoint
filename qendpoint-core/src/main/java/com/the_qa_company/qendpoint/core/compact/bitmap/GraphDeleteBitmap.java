package com.the_qa_company.qendpoint.core.compact.bitmap;

import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.util.io.Closer;

import java.io.Closeable;
import java.io.IOException;

/**
 * Bitmap to delete inside a graph, all the ids are mapped on a bitmap
 * with the formula <pre>(id, graph) -> id * graphs + graph</pre>
 *
 * @author Antoine Willerval
 */
public class GraphDeleteBitmap implements SimpleModifiableBitmap, Closeable {
	public static GraphDeleteBitmap memory(long graphs, long size) {
		return new GraphDeleteBitmap(MultiRoaringBitmap.memory(size * graphs), graphs);
	}

	public static GraphDeleteBitmap wrap(Bitmap bitmap, long graphs) {
		return new GraphDeleteBitmap(bitmap, graphs);
	}

	private final Bitmap store;
	private final long graphs;

	private GraphDeleteBitmap(Bitmap store, long graphs) {
		this.store = store;
		this.graphs = graphs;
	}

	/**
	 * access a bit in a graph
	 *
	 * @param graph    graph
	 * @param position position
	 * @return bit value
	 */
	public boolean access(long graph, long position) {
		return access(position * graphs + graph);
	}

	/**
	 * set a bit in a graph
	 *
	 * @param graph    graph
	 * @param position position
	 * @param value    value
	 * @throws ClassCastException if the wrapped bitmap isn't a modifiable bitmap
	 */
	public void set(int graph, long position, boolean value) {
		set(position * graphs + graph, value);
	}

	@Override
	public boolean access(long position) {
		return store.access(position);
	}

	@Override
	public void set(long position, boolean value) {
		((ModifiableBitmap) store).set(position, value);
	}

	@Override
	public long getNumBits() {
		return store.getNumBits();
	}

	@Override
	public long getSizeBytes() {
		return store.getSizeBytes();
	}

	@Override
	public String getType() {
		return store.getType();
	}

	@Override
	public void append(boolean value) {
		throw new NotImplementedException();
	}

	@Override
	public void close() throws IOException {
		Closer.closeSingle(store);
	}
}
