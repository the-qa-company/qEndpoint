package com.the_qa_company.qendpoint.core.dictionary.impl;

import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionarySection;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

/**
 * Implementation of {@link DictionarySectionPrivate} from a
 * {@link DictionarySection}, if the section isn't private, all the private
 * operations will return a {@link NotImplementedException}
 *
 * @author Antoine Willerval
 */
public class UnmodifiableDictionarySectionPrivate implements DictionarySectionPrivate {

	public static DictionarySectionPrivate of(DictionarySection handle) {
		if (handle instanceof DictionarySectionPrivate) {
			return (DictionarySectionPrivate) handle;
		}
		return new UnmodifiableDictionarySectionPrivate(handle);
	}

	private final DictionarySection wrapper;

	private UnmodifiableDictionarySectionPrivate(DictionarySection wrapper) {
		this.wrapper = wrapper;
	}

	@Override
	public long locate(CharSequence s) {
		return wrapper.locate(s);
	}

	@Override
	public CharSequence extract(long pos) {
		return wrapper.extract(pos);
	}

	@Override
	public long size() {
		return wrapper.size();
	}

	@Override
	public long getNumberOfElements() {
		return wrapper.getNumberOfElements();
	}

	@Override
	public Iterator<? extends CharSequence> getSortedEntries() {
		return wrapper.getSortedEntries();
	}

	@Override
	public void close() throws IOException {
		wrapper.close();
	}

	@Override
	public void load(TempDictionarySection other, ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public void load(InputStream input, ProgressListener listener) throws IOException {
		throw new NotImplementedException();
	}
}
