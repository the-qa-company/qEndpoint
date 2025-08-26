package com.the_qa_company.qendpoint.utils;

import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.impl.HDTImpl;
import com.the_qa_company.qendpoint.core.header.Header;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import com.the_qa_company.qendpoint.core.triples.Triples;
import com.the_qa_company.qendpoint.core.util.io.IntegrityObject;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Wrapper class for HDT to avoid calling twice the close method
 *
 * @author Antoine Willerval
 */
public class CloseSafeHDT implements HDT, IntegrityObject {
	private final HDT hdt;
	private boolean closed;

	/**
	 * create a close safe hdt from another hdt, the behavior is undefined if
	 * the hdt is already closed
	 *
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
	public IteratorTripleString search(CharSequence subject, CharSequence predicate, CharSequence object)
			throws NotFoundException {
		return hdt.search(subject, predicate, object);
	}

	@Override
	public IteratorTripleString search(CharSequence subject, CharSequence predicate, CharSequence object,
			CharSequence graph) throws NotFoundException {
		return hdt.search(subject, predicate, object, graph);
	}

	@Override
	public IteratorTripleString search(CharSequence subject, CharSequence predicate, CharSequence object,
			int searchOrderMask) throws NotFoundException {
		return hdt.search(subject, predicate, object, searchOrderMask);
	}

	@Override
	public IteratorTripleString search(CharSequence subject, CharSequence predicate, CharSequence object,
			CharSequence graph, int searchOrderMask) throws NotFoundException {
		return hdt.search(subject, predicate, object, graph, searchOrderMask);
	}

	@Override
	public void checkIntegrity(ProgressListener listener) throws IOException {
		IntegrityObject.checkObjectIntegrity(listener, hdt);
	}
}
