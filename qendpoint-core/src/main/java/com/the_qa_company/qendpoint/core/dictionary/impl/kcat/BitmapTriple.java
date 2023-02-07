package com.the_qa_company.qendpoint.core.dictionary.impl.kcat;

import com.the_qa_company.qendpoint.core.compact.bitmap.ModifiableBitmap;
import com.the_qa_company.qendpoint.core.util.io.Closer;

import java.io.Closeable;
import java.io.IOException;

public class BitmapTriple implements Closeable {
	private final ModifiableBitmap subjects;
	private final ModifiableBitmap predicates;
	private final ModifiableBitmap objects;

	public BitmapTriple(ModifiableBitmap subjects, ModifiableBitmap predicates, ModifiableBitmap objects) {
		this.subjects = subjects;
		this.predicates = predicates;
		this.objects = objects;
	}

	public ModifiableBitmap getSubjects() {
		return subjects;
	}

	public ModifiableBitmap getPredicates() {
		return predicates;
	}

	public ModifiableBitmap getObjects() {
		return objects;
	}

	@Override
	public void close() throws IOException {
		Closer.closeAll(subjects, predicates, objects);
	}
}
