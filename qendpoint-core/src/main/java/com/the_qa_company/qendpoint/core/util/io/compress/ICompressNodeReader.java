package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.IndexNodeDeltaMergeExceptionIterator;
import com.the_qa_company.qendpoint.core.triples.IndexedNode;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

public interface ICompressNodeReader extends ExceptionIterator<IndexedNode, IOException>,
		IndexNodeDeltaMergeExceptionIterator.IndexNodeDeltaFetcher<IOException>, Closeable {

	static ICompressNodeReader of(InputStream is, boolean raw) throws IOException {
		if (raw) {
			return new CompressNodeReaderRaw(is);
		} else {
			return new CompressNodeReader(is);
		}
	}

	void checkComplete() throws IOException;
	IndexedNode read() throws IOException;
	void pass();
}
