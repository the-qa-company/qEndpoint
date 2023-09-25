package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.exceptions.CRCException;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

/**
 * Class to read and map pre-mapped a triples file
 *
 * @author Antoine Willerval
 */
public class CompressTripleReader implements ExceptionIterator<TripleID, IOException>, Closeable {
	private final CRCInputStream stream;
	private final TripleID next = new TripleID(-1, -1, -1);
	private boolean read = false, end = false;
	private final boolean quad;

	public CompressTripleReader(InputStream stream) throws IOException {
		this.stream = new CRCInputStream(stream, new CRC32());
		int flags = this.stream.read();
		this.quad = (flags & CompressTripleWriter.FLAG_QUAD) != 0;
	}

	@Override
	public boolean hasNext() throws IOException {
		if (read) {
			return true;
		}

		// the reader is empty, null end triple
		if (end) {
			return false;
		}

		long s, p, o;

		if (quad) {
			long g;
			do {
				s = VByte.decode(stream);
				p = VByte.decode(stream);
				o = VByte.decode(stream);
				g = VByte.decode(stream);
				// continue to read to avoid duplicated triples
			} while (s == next.getSubject() && p == next.getPredicate() && o == next.getObject() && g == next.getGraph());

			return !setAllOrEnd(s, p, o, g);
		} else {
			do {
				s = VByte.decode(stream);
				p = VByte.decode(stream);
				o = VByte.decode(stream);
				// continue to read to avoid duplicated triples
			} while (s == next.getSubject() && p == next.getPredicate() && o == next.getObject());

			return !setAllOrEnd(s, p, o, 0);
		}
	}

	private boolean setAllOrEnd(long s, long p, long o, long g) throws IOException {
		if (end) {
			// already completed
			return true;
		}
		if (s == 0 || p == 0 || o == 0) {
			// check triples validity
			if (s != 0 || p != 0 || o != 0) {
				throw new IOException("Triple got null node, but not all the nodes are 0! " + s + " " + p + " " + o);
			}
			if (!stream.readCRCAndCheck()) {
				throw new CRCException("CRC Error while reading PreMapped triples.");
			}
			// set to true to avoid reading again the CRC
			end = true;
			return true;
		}
		if (quad && g == 0) {
			throw new IOException("Triple got null graph, but not all the nodes are 0! " + s + " " + p + " " + o);
		}
		// map the triples to the end id, compute the shared with the end shared
		// size
		if (quad) {
			next.setAll(s, p, o, g);
		} else {
			next.setAll(s, p, o);
		}
		read = true;
		return false;
	}

	@Override
	public TripleID next() throws IOException {
		if (!hasNext()) {
			return null;
		}
		read = false;
		return next;
	}

	@Override
	public void close() throws IOException {
		stream.close();
	}
}
