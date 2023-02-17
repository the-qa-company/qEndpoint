package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.util.io.CountInputStream;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface PredicateIndex {
	long getNumOcurrences(long pred);

	long getBase(long pred);

	long getOccurrence(long base, long occ);

	void load(InputStream in) throws IOException;

	void save(OutputStream in) throws IOException;

	void mapIndex(CountInputStream input, File f, ProgressListener listener) throws IOException;

	void generate(ProgressListener listener, HDTOptions spec, Dictionary dictionary);

	void close() throws IOException;
}
