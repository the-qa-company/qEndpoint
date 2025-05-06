package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.triples.IndexedNode;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public interface ICompressNodeWriter extends Closeable {
	static ICompressNodeWriter of(OutputStream os, long size, boolean raw) throws IOException {
		if (raw) {
			return new CompressNodeWriterRaw(os, size);
		} else {
			return new CompressNodeWriter(os, size);
		}
	}
	void appendNode(IndexedNode node) throws IOException;
	void writeCRC() throws IOException;
}
