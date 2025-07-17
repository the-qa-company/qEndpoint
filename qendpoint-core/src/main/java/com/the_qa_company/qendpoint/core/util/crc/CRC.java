package com.the_qa_company.qendpoint.core.util.crc;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.io.CloseMappedByteBuffer;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface CRC extends Comparable<CRC> {
	/**
	 * Update this CRC with the content of the buffer, from offset, using length
	 * bytes.
	 */
	void update(byte[] buffer, int offset, int length);

	void update(CloseMappedByteBuffer buffer, int offset, int length);

	/**
	 * Update the CRC with the specified byte
	 */
	void update(byte data);

	/**
	 * Write this CRC to an Output Stream
	 */
	void writeCRC(OutputStream out) throws IOException;

	/**
	 * Write this CRC to a File Channel, return the written CRC size
	 */
	int writeCRC(CloseMappedByteBuffer buffer, int offset) throws IOException;

	/**
	 * Read CRC from InputStream and compare it to this.
	 *
	 * @param in InputStream
	 * @return true if the checksum is the same, false if checksum error.
	 * @throws IOException
	 */
	boolean readAndCheck(InputStream in) throws IOException;

	/**
	 * Read CRC from Buffer and compare it to this.
	 *
	 * @param buffer Buffer
	 * @param offset Offset in the buffer
	 * @return true if the checksum is the same, false if checksum error.
	 * @throws IOException ioe
	 */
	boolean readAndCheck(CloseMappedByteBuffer buffer, int offset) throws IOException;

	default void update(InputStream is, long len) throws IOException {
		update(is, len, null);
	}

	default void update(InputStream is, long len, ProgressListener listener) throws IOException {
		ProgressListener il = ProgressListener.ofNullable(listener);
		if (len <= 0)
			return; // nothing to see
		byte[] buffer = new byte[0x1000];
		long remaining = len;
		long newUpdate = len;
		while (remaining > 0) {
			int toread = (int) Math.min(buffer.length, remaining);
			int r = is.readNBytes(buffer, 0, toread);
			if (r == 0)
				throw new EOFException();
			update(buffer, 0, r);
			remaining -= r;

			if (remaining < newUpdate) {
				listener.notifyProgress((float) (100 * (len - remaining)) / len,
						"updating crc " + (len - remaining) + "/" + len);
				newUpdate = remaining - len / 10;
			}
		}
		listener.notifyProgress(100, "crc updated");
	}

	/**
	 * Get checksum value.
	 */
	long getValue();

	/**
	 * Reset the checksum to the initial value.
	 */
	void reset();

	/**
	 * @return the number of bytes used to store it
	 */
	int sizeof();
}
