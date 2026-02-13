package com.the_qa_company.qendpoint.core.iterator.utils;

import com.the_qa_company.qendpoint.core.triples.TripleString;

import java.util.Iterator;

/**
 * Iterator to split an iterator stream into multiple files, the iterator return
 * {@link #hasNext()} == true once the first file is returned, then the
 * {@link #hasNewFile()} should be called to check if another file can be
 * created and re-allow {@link #hasNext()} to return true
 *
 * @author Antoine Willerval
 */
public class FileTripleIterator extends FileChunkIterator<TripleString> {
	public static long estimateSize(TripleString tripleString) {
		// Approximation used for chunk sizing: cheap and monotonic-ish.
		long size = tripleString.getSubject().length() + tripleString.getPredicate().length()
				+ tripleString.getObject().length();
		CharSequence graph = tripleString.getGraph();
		if (graph != null && !graph.isEmpty()) {
			size += graph.length();
		}
		// Rough overhead for separators and N-Triples syntax.
		return size + 16;
	}

	/**
	 * create a file iterator from a stream and a max size
	 *
	 * @param it      the iterator
	 * @param maxSize the maximum size of each file, this size is estimated, so
	 *                files can be bigger.
	 */
	public FileTripleIterator(Iterator<TripleString> it, long maxSize) {
		super(it, maxSize, FileTripleIterator::estimateSize);
	}

}
