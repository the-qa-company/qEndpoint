package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Writer for {@link Pair}, if the stream is stored, the file size will be
 * reduced
 *
 * @author Antoine Willerval
 */
public class PairWriter implements Closeable {
	private final CRCOutputStream out;
	private final long size;
	private long index;
	private final Pair lastValue = new Pair();

	public PairWriter(OutputStream writer, long size) throws IOException {
		if (size < 0) {
			throw new IllegalArgumentException("Negative size!");
		}
		this.out = new CRCOutputStream(writer, new CRC32());
		VByte.encode(this.out, size);
		this.size = size;
	}

	public void append(Pair pair) throws IOException {
		if (index >= size) {
			throw new IllegalArgumentException("add more elements than size!");
		}
		// encode the delta with the previous pair to reduce the size used by
		// the pair on disk
		// if the stream isn't sorted, it will add at worse 3 bits / pair, if
		// the stream is sorted,
		// the file's size will be drastically reduced
		VByte.encodeSigned(out, pair.predicatePosition - lastValue.predicatePosition);
		VByte.encodeSigned(out, pair.object - lastValue.object);
		VByte.encodeSigned(out, pair.predicate - lastValue.predicate);

		// save previous value
		lastValue.setAll(pair);

		index++;
	}

	public void writeCRC() throws IOException {
		out.writeCRC();
	}

	@Override
	public void close() throws IOException {
		try {
			if (index != size) {
				throw new IllegalArgumentException("less elements than size were added!");
			}
			writeCRC();
		} finally {
			out.close();
		}
	}
}
