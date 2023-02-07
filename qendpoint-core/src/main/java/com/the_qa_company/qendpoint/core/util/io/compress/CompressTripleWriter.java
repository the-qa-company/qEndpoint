package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.triples.IndexedTriple;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Class to write pre-mapped triples file
 *
 * @author Antoine Willerval
 */
public class CompressTripleWriter implements Closeable {
	private final CRCOutputStream out;

	public CompressTripleWriter(OutputStream writer) {
		this.out = new CRCOutputStream(writer, new CRC32());
	}

	/**
	 * write a indexed triple into an output
	 *
	 * @param triple the triple to write
	 * @throws java.io.IOException write exception
	 */
	public void appendTriple(IndexedTriple triple) throws IOException {
		VByte.encode(out, triple.getSubject().getIndex());
		VByte.encode(out, triple.getPredicate().getIndex());
		VByte.encode(out, triple.getObject().getIndex());
	}

	/**
	 * write a indexed triple into an output
	 *
	 * @param triple the triple to write
	 * @throws java.io.IOException write exception
	 */
	public void appendTriple(TripleID triple) throws IOException {
		VByte.encode(out, triple.getSubject());
		VByte.encode(out, triple.getPredicate());
		VByte.encode(out, triple.getObject());
	}

	/**
	 * Write an end triple and a CRC to complete the writer
	 *
	 * @throws IOException write error
	 */
	public void writeCRC() throws IOException {
		VByte.encode(out, 0);
		VByte.encode(out, 0);
		VByte.encode(out, 0);
		out.writeCRC();
	}

	@Override
	public void close() throws IOException {
		writeCRC();
		out.close();
	}
}
