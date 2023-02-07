package com.the_qa_company.qendpoint.core.util.crc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface CRC extends Comparable<CRC> {
	/**
	 * Update this CRC with the content of the buffer, from offset, using length
	 * bytes.
	 */
	void update(byte[] buffer, int offset, int length);

	/** Update the CRC with the specified byte */
	void update(byte data);

	/** Write this CRC to an Output Stream */
	void writeCRC(OutputStream out) throws IOException;

	/**
	 * Read CRC from InputStream and compare it to this.
	 *
	 * @param in InputStream
	 * @return true if the checksum is the same, false if checksum error.
	 * @throws IOException
	 */
	boolean readAndCheck(InputStream in) throws IOException;

	/**
	 * Get checksum value.
	 */
	long getValue();

	/**
	 * Reset the checksum to the initial value.
	 */
	void reset();
}
