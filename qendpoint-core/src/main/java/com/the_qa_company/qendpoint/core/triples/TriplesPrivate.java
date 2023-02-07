package com.the_qa_company.qendpoint.core.triples;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.iterator.SuppliableIteratorTripleID;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.options.ControlInfo;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.util.io.CountInputStream;

public interface TriplesPrivate extends Triples {
	/**
	 * Serializes the triples to an OutputStream
	 *
	 * @param output The OutputStream to save the triples to
	 */
	void save(OutputStream output, ControlInfo ci, ProgressListener listener) throws IOException;

	/**
	 * Iterates over all triples that match the pattern.
	 *
	 * @param pattern The pattern to match against
	 * @return IteratorTripleID
	 */
	SuppliableIteratorTripleID search(TripleID pattern);

	/**
	 * Loads the structure from an InputStream
	 *
	 * @param input The InputStream to load the file from
	 * @throws IOException
	 */
	void load(InputStream input, ControlInfo ci, ProgressListener listener) throws IOException;

	void mapFromFile(CountInputStream in, File f, ProgressListener listener) throws IOException;

	/**
	 * Generates the associated Index
	 *
	 * @param listener
	 */
	void generateIndex(ProgressListener listener, HDTOptions spec, Dictionary dictionary) throws IOException;

	/**
	 * Loads the associated Index from an InputStream
	 *
	 * @param input The InputStream to load the index from
	 * @throws IOException
	 */
	void loadIndex(InputStream input, ControlInfo ci, ProgressListener listener) throws IOException;

	/**
	 * Loads the associated Index from an InputStream
	 *
	 * @param input The InputStream to load the index from
	 * @throws IOException
	 */
	void mapIndex(CountInputStream input, File f, ControlInfo ci, ProgressListener listener) throws IOException;

	/**
	 * Saves the associated Index to an OutputStream
	 *
	 * @param output The OutputStream to save the index
	 * @throws IOException
	 */
	void saveIndex(OutputStream output, ControlInfo ci, ProgressListener listener) throws IOException;

	/**
	 * Loads triples from another Triples Structure
	 *
	 * @param input The TempTriples input to load from
	 */
	void load(TempTriples input, ProgressListener listener);

	/**
	 * Gets the currently set order(TripleComponentOrder)
	 */
	TripleComponentOrder getOrder();
}
