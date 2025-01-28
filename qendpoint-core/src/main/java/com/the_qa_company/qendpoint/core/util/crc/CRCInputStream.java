package com.the_qa_company.qendpoint.core.util.crc;

import com.the_qa_company.qendpoint.core.exceptions.CRCException;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * FilterInputStream that validates a checksum on the fly.
 *
 * @author mario.arias
 */

public class CRCInputStream extends FilterInputStream {
	protected CRC crc;

	/**
	 * Create a CRCInputStream using the specified CRC generator
	 *
	 * @param in
	 * @param crc
	 */
	public CRCInputStream(InputStream in, CRC crc) {
		super(in);
		this.crc = crc;
	}

	/**
	 * Set a new CRC generator from the current read byte onwards.
	 *
	 * @param crc
	 */
	public void setCRC(CRC crc) {
		this.crc = crc;
	}

	public CRC getCRC() {
		return this.crc;
	}

	/**
	 * Read CRC from the stream itself, and check that it matches with the
	 * generated from the previous bytes.
	 *
	 * @return True if the CRC's match, False on CRC Error.
	 * @throws IOException
	 */
	public boolean readCRCAndCheck() throws IOException {
		return crc.readAndCheck(in);
	}
	/**
	 * Read CRC from the stream itself, and check that it matches with the
	 * generated from the previous bytes.
	 *
	 * @throws IOException if bad crc
	 */
	public void assertCRC() throws IOException {
		if (!crc.readAndCheck(in)) {
			throw new CRCException("Invalid crc exception");
		}
	}

	@Override
	public int read() throws IOException {
		int val = in.read();
		crc.update((byte) (val & 0xFF));
		return val;
	}

	@Override
	public int read(byte[] b) throws IOException {
		return this.read(b, 0, b.length);
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		int ret = in.read(b, off, len);
		if (ret == -1)
			return -1;
		crc.update(b, off, ret);
		return ret;
	}
}
