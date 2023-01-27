package com.the_qa_company.qendpoint.core.hdt;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;

/**
 * HDT Operations that are using internally from the implementation.
 *
 * @author mario.arias
 */

public interface HDTPrivate extends HDT {
	/**
	 * Loads a HDT file
	 *
	 * @param input InputStream to read from
	 */
	void loadFromHDT(InputStream input, ProgressListener listener) throws IOException;

	/**
	 * Loads a HDT file
	 *
	 * @param fileName Filepath String to read from
	 */
	void loadFromHDT(String fileName, ProgressListener listener) throws IOException;

	void mapFromHDT(File f, long offset, ProgressListener listener) throws IOException;

	/**
	 * Generates any additional index needed to solve all triple patterns more
	 * efficiently
	 *
	 * @param listener A listener to be notified of the progress.
	 */
	void loadOrCreateIndex(ProgressListener listener, HDTOptions disk) throws IOException;

	void populateHeaderStructure(String baseUri);
}
