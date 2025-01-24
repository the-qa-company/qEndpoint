package com.the_qa_company.qendpoint.core.dictionary;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;

public interface DictionarySectionPrivate extends DictionarySection {
	/**
	 * Load entries from another dictionary section.
	 *
	 * @param other
	 * @param listener
	 */
	void load(TempDictionarySection other, ProgressListener listener);

	/**
	 * Load entries from another dictionary section.
	 *
	 * @param it       iterator
	 * @param count    count
	 * @param listener listener
	 */
	void load(Iterator<? extends CharSequence> it, long count, ProgressListener listener);

	/**
	 * Serialize dictionary section to a stream.
	 *
	 * @param output
	 * @param listener
	 * @throws IOException
	 */
	void save(OutputStream output, ProgressListener listener) throws IOException;

	/**
	 * Load dictionary section from a stream.
	 *
	 * @param input
	 * @param listener
	 * @throws IOException
	 */
	void load(InputStream input, ProgressListener listener) throws IOException;

	default void save(Path res) throws IOException {
		save(res, ProgressListener.ignore());
	}

	default void save(Path res, ProgressListener listener) throws IOException {
		try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(res))) {
			save(os, listener);
		}
	}

	default boolean isIndexedSection() {
		return true;
	}
}
