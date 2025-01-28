package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.iterator.utils.FetcherExceptionIterator;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;
import com.the_qa_company.qendpoint.core.util.io.BitStreamReader;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

/**
 * Compact triple reader, use {@link BitTripleWriter} to pack.
 *
 * @author Antoine Willerval
 */
public class BitTripleReader extends FetcherExceptionIterator<TripleID, IOException> implements Closeable {
	private final TripleID temp = new TripleID();
	private final CRCInputStream crcIn;
	final BitStreamReader reader;
	private final int ySize;
	private final int zSize;

	public BitTripleReader(InputStream in) throws IOException {
		this.crcIn = new CRCInputStream(in, new CRC32());
		this.reader = new BitStreamReader(crcIn, false);
		this.ySize = (int)reader.readNumber(6);
		this.zSize = (int)reader.readNumber(6);
	}

	@Override
	protected TripleID getNext() throws IOException {
		// has next z value?
		if (reader.readBit()) {
			temp.setObject(reader.readNumber(zSize));
			return temp;
		}
		// has next y value?
		if (reader.readBit()) {
			temp.setPredicate(reader.readNumber(ySize));
			temp.setObject(reader.readNumber(zSize));
			return temp;
		}

		// goto to the next subject
		temp.setSubject(temp.getSubject() + 1);
		temp.setPredicate(reader.readNumber(ySize));
		temp.setObject(reader.readNumber(zSize));

		if (temp.getPredicate() != 0 && temp.getObject() != 0) {
			return temp;
		}

		assert temp.getPredicate() == 0 && temp.getObject() == 0 : temp;

		// end value, checking crc
		reader.close();
		crcIn.assertCRC();

		return null;
	}

	@Override
	public void close() throws IOException {
		crcIn.close();
	}
}
