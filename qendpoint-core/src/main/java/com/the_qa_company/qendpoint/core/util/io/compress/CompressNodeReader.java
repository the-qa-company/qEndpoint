package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.exceptions.CRCException;
import com.the_qa_company.qendpoint.core.triples.IndexedNode;
import com.the_qa_company.qendpoint.core.utils.DebugOrderNodeIterator;
import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.IndexNodeDeltaMergeExceptionIterator;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;
import com.the_qa_company.qendpoint.core.util.string.ReplazableString;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

/**
 * Class to read a compress node file
 *
 * @author Antoine Willerval
 */
public class CompressNodeReader implements ExceptionIterator<IndexedNode, IOException>,
		IndexNodeDeltaMergeExceptionIterator.IndexNodeDeltaFetcher<IOException>, Closeable {
	private final CRCInputStream stream;
	private final long size;
	private long index;
	private int delta;
	private boolean waiting;
	private final IndexedNode last;
	private final ReplazableString tempString;
	private final Consumer<IndexedNode> consumer;

	private final boolean stringsLiterals;

	public CompressNodeReader(InputStream stream, boolean stringLiterals) throws IOException {
		this.stream = new CRCInputStream(stream, new CRC8());
		this.size = VByte.decode(this.stream);
		if (!this.stream.readCRCAndCheck()) {
			throw new CRCException("CRC Error while merging Section Plain Front Coding Header.");
		}
		this.stream.setCRC(new CRC32());
		this.tempString = new ReplazableString();
		this.last = new IndexedNode(tempString, -1);
		this.stringsLiterals = stringLiterals;
		consumer = stringLiterals ? DebugOrderNodeIterator.of("stream", true) : (s) -> {};
	}

	@Override
	public long getSize() {
		return size;
	}

	public void checkComplete() throws IOException {
		if (!this.stream.readCRCAndCheck()) {
			throw new CRCException("CRC Error while merging Section Plain Front Coding Header.");
		}
	}

	/**
	 * @return the next element without passing to the next element
	 * @throws IOException reading exception
	 */
	public IndexedNode read() throws IOException {
		if (waiting) {
			return last;
		}
		delta = (int) VByte.decode(stream);
		if (stringsLiterals) {
			tempString.replace2(stream, delta);
		} else {
			int len = (int) VByte.decode(stream);
			tempString.replaceLen(stream, delta, len);
		}
		long index = VByte.decode(stream);
		last.setIndex(index);
		consumer.accept(last);
		waiting = true;
		return last;
	}

	/**
	 * pass to the next element, mandatory with {@link #read()}
	 */
	public void pass() {
		waiting = false;
		index++;
	}

	@Override
	public IndexedNode next() throws IOException {
		IndexedNode node = read();
		pass();
		return node;
	}

	@Override
	public IndexedNode fetchNode() throws IOException {
		if (hasNext()) {
			return next();
		} else {
			return null;
		}
	}

	@Override
	public int lastDelta() {
		return delta;
	}

	@Override
	public boolean hasNext() throws IOException {
		return index < size;
	}

	@Override
	public void close() throws IOException {
		stream.close();
	}

}
