package com.the_qa_company.qendpoint.core.dictionary.impl.section;

import com.the_qa_company.qendpoint.core.dictionary.TempDictionarySection;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

/**
 * dictionary section assuming the {@link #getSortedEntries()} or the
 * {@link #getEntries()} will only be called once, it will return an
 * {@link java.lang.IllegalArgumentException} otherwise.
 *
 * @author Antoine Willerval
 */
public class OneReadDictionarySection implements TempDictionarySection {
	private final long size;
	private final AtomicReference<Iterator<? extends CharSequence>> ref = new AtomicReference<>();

	public OneReadDictionarySection(Iterator<? extends CharSequence> reader, long size) {
		ref.set(reader);
		this.size = size;
	}

	@Override
	public long add(CharSequence str) {
		throw new NotImplementedException();
	}

	@Override
	public void remove(CharSequence str) {
		throw new NotImplementedException();
	}

	@Override
	public void sort() {
		throw new NotImplementedException();
	}

	@Override
	public void clear() {
		throw new NotImplementedException();
	}

	@Override
	public boolean isSorted() {
		return true;
	}

	@Override
	public Iterator<? extends CharSequence> getEntries() {
		Iterator<? extends CharSequence> it = ref.getAndSet(null);

		if (it == null) {
			throw new IllegalArgumentException("This dictionary has already been get");
		}

		return it;
	}

	@Override
	public long locate(CharSequence s) {
		throw new NotImplementedException();
	}

	@Override
	public CharSequence extract(long pos) {
		throw new NotImplementedException();
	}

	@Override
	public long size() {
		return size;
	}

	@Override
	public long getNumberOfElements() {
		return size;
	}

	@Override
	public Iterator<? extends CharSequence> getSortedEntries() {
		return getEntries();
	}

	@Override
	public void close() throws IOException {
		ref.getAndSet(null);
	}
}
