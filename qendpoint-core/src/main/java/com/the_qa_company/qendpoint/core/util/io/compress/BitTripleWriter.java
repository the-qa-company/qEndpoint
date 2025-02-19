package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.io.BitStreamWriter;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import static java.lang.String.format;

/**
 * Compact triple write, use
 * {@link com.the_qa_company.qendpoint.core.util.BitUtil#log2(long)} to find the
 * y and z sizes use {@link BitTripleReader} to unpack. use
 * {@link #appendTriple(TripleID)} or {@link #appendTriple(long, long, long)} to
 * add a new triple, the subject ids should be incremental (1, 2, etc.). At the
 * end call {@link #end()} to end the stream. {@link #close()} is only closing
 * the {@link OutputStream}.
 *
 * @author Antoine Willerval
 */
public class BitTripleWriter implements Closeable {
	private final CRCOutputStream crcOut;
	final BitStreamWriter writer;
	private final int ySize;
	private final int zSize;
	public long lastX;
	public long lastY;

	public BitTripleWriter(OutputStream os, int ySize, int zSize) throws IOException {
		crcOut = new CRCOutputStream(os, new CRC32());
		writer = new BitStreamWriter(crcOut, false);
		if (ySize < 0 || ySize >= 64) {
			throw new IllegalArgumentException(format("invalid y size: %d", ySize));
		}
		if (zSize < 0 || zSize >= 64) {
			throw new IllegalArgumentException(format("invalid z size: %d", zSize));
		}
		this.ySize = ySize;
		this.zSize = zSize;
		// add the sizes in the header (2**6=64 btw)
		writer.writeLong(ySize, 6);
		writer.writeLong(zSize, 6);
	}

	/**
	 * append a new triple id
	 *
	 * @param id triple id
	 * @throws IOException write error
	 */
	public void appendTriple(TripleID id) throws IOException {
		appendTriple(id.getSubject(), id.getPredicate(), id.getObject());
	}

	/**
	 * append a new triple
	 *
	 * @param s subject
	 * @param p predicate
	 * @param o object
	 * @throws IOException write error
	 */
	public void appendTriple(long s, long p, long o) throws IOException {
		assert !(s == 0 || p == 0 || o == 0) : "empty triple " + s + "," + p + "," + o;
		if (lastX != s) {
			if (lastX + 1 != s) {
				throw new IllegalArgumentException(
						format("Invalid subject order, last subject was %d, but new is (%d, %d, %d)", lastX, s, p, o));
			}
			// close previous yz block
			writer.writeBit(false); // end block y
			writer.writeBit(false); // end block x
			// new x block
			writer.writeLong(p, ySize);
			writer.writeLong(o, zSize);
			lastX = s;
			lastY = p;
		} else if (lastY != p) {
			writer.writeBit(false); // end block y
			writer.writeBit(true); // continue block x
			// new y block
			writer.writeLong(p, ySize);
			writer.writeLong(o, zSize);
			lastY = p;
		} else {
			writer.writeBit(true); // continue block y
			writer.writeLong(o, zSize);
		}
	}

	/**
	 * end the stream
	 *
	 * @throws IOException write error
	 */
	public void end() throws IOException {
		writer.writeBit(false); // end block z
		writer.writeBit(false); // end block y
		writer.writeLong(0, ySize); // empty y
		writer.writeLong(0, zSize); // empty z
		// the stream is at the end, we can close it and write a CRC after the
		// alignment
		writer.close();
		crcOut.writeCRC();
	}

	@Override
	public void close() throws IOException {
		crcOut.close();
	}
}
