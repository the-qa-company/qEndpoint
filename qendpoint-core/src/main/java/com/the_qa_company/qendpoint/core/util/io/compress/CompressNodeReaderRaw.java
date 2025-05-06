package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.exceptions.CRCException;
import com.the_qa_company.qendpoint.core.triples.IndexedNode;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.utils.DebugOrderNodeIterator;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;
import com.the_qa_company.qendpoint.core.util.string.ReplazableString;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

/**
 * Class to read a compress node file
 *
 * @author Antoine Willerval
 */
public class CompressNodeReaderRaw implements ICompressNodeReader {
	private final CRCInputStream stream;
	private final long size;
	private long index;
	private boolean waiting;
	private final IndexedNode last;
	private final ReplazableString tempString;
	private final Consumer<IndexedNode> consumer;

	public CompressNodeReaderRaw(InputStream stream) throws IOException {
		this.stream = new CRCInputStream(stream, new CRC8());
		this.size = IOUtil.readLong(this.stream);
		if (!this.stream.readCRCAndCheck()) {
			throw new CRCException("CRC Error while merging Section Plain Front Coding Header. size:" + this.size);
		}
		this.stream.setCRC(new CRC32());
		this.tempString = new ReplazableString();
		this.last = new IndexedNode(tempString, -1);
		consumer = DebugOrderNodeIterator.of("stream", true);
	}

	@Override
	public long getSize() {
		return size;
	}

	@Override
	public void checkComplete() throws IOException {
		if (!this.stream.readCRCAndCheck()) {
			throw new CRCException("CRC Error while merging Section Plain Front Coding Header.");
		}
	}

	/**
	 * @return the next element without passing to the next element
	 * @throws IOException reading exception
	 */
	@Override
	public IndexedNode read() throws IOException {
		if (waiting) {
			return last;
		}
		int len = IOUtil.readInt(stream);
		tempString.replace(stream, 0, len);
		long index = IOUtil.readLong(stream);
		last.setIndex(index);
		consumer.accept(last);
		waiting = true;
		return last;
	}

	/**
	 * pass to the next element, mandatory with {@link #read()}
	 */
	@Override
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
		return 0;
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
