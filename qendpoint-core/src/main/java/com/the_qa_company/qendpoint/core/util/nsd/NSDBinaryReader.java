package com.the_qa_company.qendpoint.core.util.nsd;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;

import java.io.IOException;
import java.io.InputStream;

/**
 * Binary reader implementation for the {@link NamespaceData} object
 *
 * @author Antoine Willerval
 */
public interface NSDBinaryReader {
	/**
	 * @return the version of the reader
	 */
	byte version();

	/**
	 * read the data from a stream
	 *
	 * @param data     namespace data to fill
	 * @param stream   stream to read
	 * @param listener listener to handle
	 * @throws IOException reading exception
	 */
	void readData(NamespaceData data, InputStream stream, ProgressListener listener) throws IOException;

}
