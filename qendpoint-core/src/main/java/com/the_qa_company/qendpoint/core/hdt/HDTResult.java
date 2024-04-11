package com.the_qa_company.qendpoint.core.hdt;

import com.the_qa_company.qendpoint.core.util.io.Closer;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * HDT operation result, can contain one or multiple HDT files.
 *
 * @author Antoine Willerval
 */
public class HDTResult implements Closeable {
	/**
	 * Create result from one HDT
	 * @param hdt hdt
	 * @return result
	 */
	public static HDTResult of(HDT hdt) {
		return new HDTResult(List.of(hdt));
	}

	/**
	 * Create result from HDTs list
	 * @param hdts hdts
	 * @return results
	 */
	public static HDTResult of(List<HDT> hdts) {
		return new HDTResult(hdts);
	}

	private final List<HDT> hdts;

	private HDTResult(List<HDT> hdts) {
		this.hdts = hdts;
	}

	/**
	 * @return hdt files count
	 */
	public int getHdtCount() {
		return hdts.size();
	}

	/**
	 * @return get the result
	 */
	public HDT getHdtSinge() {
		return getHdtSinge(false);
	}
	public HDT getHdtSinge(boolean closeOnError) {
		if (getHdtCount() != 1) {
			IllegalArgumentException ex = new IllegalArgumentException("Trying to use hdt result with multiple HDTs");
			if (closeOnError) {
				try {
					close();
				} catch (Throwable t1) {
					ex.addSuppressed(t1);
				}
			}
			throw ex;
		}
		return getHdt(0);
	}

	public HDT getHdt(int index) {
		return hdts.get(index);
	}

	public void close() throws IOException {
		Closer.closeSingle(hdts);
	}

	/**
	 * @return hdts
	 */
	public List<HDT> getHdts() {
		return hdts;
	}
}
