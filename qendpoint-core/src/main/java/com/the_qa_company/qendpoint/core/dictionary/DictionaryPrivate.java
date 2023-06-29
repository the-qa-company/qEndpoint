package com.the_qa_company.qendpoint.core.dictionary;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.the_qa_company.qendpoint.core.dictionary.impl.OptimizedExtractor;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.ControlInfo;
import com.the_qa_company.qendpoint.core.util.io.CountInputStream;

public interface DictionaryPrivate extends Dictionary {
	/**
	 * Loads a dictionary from a InputStream
	 *
	 * @param input InputStream to load the dictionary from
	 * @throws IOException
	 */
	void load(InputStream input, ControlInfo ci, ProgressListener listener) throws IOException;

	void mapFromFile(CountInputStream in, File f, ProgressListener listener) throws IOException;

	/**
	 * Loads all information from another dictionary into this dictionary.
	 */
	void load(TempDictionary other, ProgressListener listener);

	/**
	 * same as {@link #load(TempDictionary, ProgressListener)} but read all the
	 * section at the same time
	 */
	void loadAsync(TempDictionary other, ProgressListener listener) throws InterruptedException;

	/**
	 * Saves the dictionary to a OutputStream
	 */
	void save(OutputStream output, ControlInfo ci, ProgressListener listener) throws IOException;

	/**
	 * @return an optimized map extractor from this dictionary
	 */
	OptimizedExtractor createOptimizedMapExtractor();
}
