package com.the_qa_company.qendpoint.core.util.crc;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * FilterOutputStream that generates a Checksum on the fly.
 *
 * @author mario.arias
 */

public class CRCOutputStream extends FilterOutputStream {
	protected CRC crc;

	/**
	 * Create a new CRCOutputStream using the selected CRC generator
	 *
	 * @param out
	 * @param crc
	 */
	public CRCOutputStream(OutputStream out, CRC crc) {
		super(out);
		this.crc = crc;
	}

	/**
	 * Set a new CRC generator from the current byte onwards.
	 *
	 * @param crc
	 */
	public void setCRC(CRC crc) {
		this.crc = crc;
	}

	/**
	 * Get the existing crc
	 */
	public CRC getCRC() {
		return this.crc;
	}

	/**
	 * Write the CRC digest in the current position of the OutputStream.
	 *
	 * @throws IOException
	 */
	public void writeCRC() throws IOException {
		crc.writeCRC(out);
	}

	@Override
	public void write(byte[] b) throws IOException {
		crc.update(b, 0, b.length);
		out.write(b);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		crc.update(b, off, len);
		out.write(b, off, len);
	}

	@Override
	public void write(int b) throws IOException {
		crc.update((byte) (b & 0xFF));
		out.write(b);
	}

}
