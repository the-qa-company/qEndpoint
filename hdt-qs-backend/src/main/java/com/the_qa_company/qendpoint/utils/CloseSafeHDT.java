package com.the_qa_company.qendpoint.utils;

import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.impl.HDTImpl;
import org.rdfhdt.hdt.header.Header;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.Triples;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Wrapper class for HDT to avoid calling twice the close method
 * @author Antoine Willerval
 */
public class CloseSafeHDT implements HDT {
	private final HDT hdt;
	private boolean closed;

	/**
	 * create a close safe hdt from another hdt, the behavior is undefined if the hdt is already closed
	 * @param hdt hdt to wrap
	 */
	public CloseSafeHDT(HDT hdt) {
		this.hdt = hdt;
		if (hdt instanceof HDTImpl) {
			this.closed = ((HDTImpl) hdt).isClosed();
		}
	}

	@Override
	public Header getHeader() {
		return hdt.getHeader();
	}

	@Override
	public Dictionary getDictionary() {
		return hdt.getDictionary();
	}

	@Override
	public Triples getTriples() {
		return hdt.getTriples();
	}

	@Override
	public void saveToHDT(OutputStream output, ProgressListener listener) throws IOException {
		hdt.saveToHDT(output, listener);
	}

	@Override
	public void saveToHDT(String fileName, ProgressListener listener) throws IOException {
		hdt.saveToHDT(fileName, listener);
	}

	@Override
	public long size() {
		return hdt.size();
	}

	@Override
	public String getBaseURI() {
		return hdt.getBaseURI();
	}

	@Override
	public void close() throws IOException {
		if (!isClosed()) {
			closed = true;
			hdt.close();
		}
	}

	/**
	 * @return if the hdt was closed
	 */
	public boolean isClosed() {
		return closed;
	}

	@Override
	public IteratorTripleString search(CharSequence subject, CharSequence predicate, CharSequence object) throws NotFoundException {
		return hdt.search(subject, predicate, object);
	}
}
