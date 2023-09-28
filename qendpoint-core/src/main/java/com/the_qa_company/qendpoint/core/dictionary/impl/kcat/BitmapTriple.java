package com.the_qa_company.qendpoint.core.dictionary.impl.kcat;

import com.the_qa_company.qendpoint.core.compact.bitmap.ModifiableBitmap;
import com.the_qa_company.qendpoint.core.util.io.Closer;

import java.io.Closeable;
import java.io.IOException;

public class BitmapTriple implements Closeable {
	private final ModifiableBitmap subjects;
	private final ModifiableBitmap predicates;
	private final ModifiableBitmap objects;
	private final ModifiableBitmap graphs;

	public BitmapTriple(ModifiableBitmap subjects, ModifiableBitmap predicates, ModifiableBitmap objects,
			ModifiableBitmap graphs) {
		this.subjects = subjects;
		this.predicates = predicates;
		this.objects = objects;
		this.graphs = graphs;
	}

	public ModifiableBitmap getSubjects() {
		return subjects;
	}

	public ModifiableBitmap getPredicates() {
		return predicates;
	}

	public ModifiableBitmap getGraphs() {
		return graphs;
	}

	public ModifiableBitmap getObjects() {
		return objects;
	}

	@Override
	public void close() throws IOException {
		Closer.closeAll(subjects, predicates, objects, graphs);
	}
}
