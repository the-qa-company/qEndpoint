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
	public static final int FLAG_QUAD = 1;
	private final CRCOutputStream out;
	private final boolean quad;

	public CompressTripleWriter(OutputStream writer, boolean quad) throws IOException {
		this.out = new CRCOutputStream(writer, new CRC32());
		this.quad = quad;
		// write quad header
		int flags = 0;

		if (quad) {
			flags |= FLAG_QUAD;
		}

		out.write(flags);
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
		if (quad) {
			VByte.encode(out, triple.getGraph().getIndex());
		}
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
		if (quad) {
			VByte.encode(out, triple.getGraph());
		}
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
		if (quad) {
			VByte.encode(out, 0);
		}
		out.writeCRC();
	}

	@Override
	public void close() throws IOException {
		writeCRC();
		out.close();
	}
}
