package com.the_qa_company.qendpoint.core.util.crc;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Buffered {@link CRCOutputStream} with a fast stop-bit (VByte-style) long
 * writer.
 * <p>
 * Properties:
 * <ul>
 * <li>Normal writes update CRC.</li>
 * <li>{@link #writeCRC()} writes digest bytes without updating CRC and
 * preserves correct ordering even when data is buffered.</li>
 * </ul>
 * </p>
 */
public final class CRCStopBitOutputStream extends CRCOutputStream implements VByte.FastOutput {
	private static final int MIN_VARINT_BYTES = 9;

	private final byte[] buf;
	private int pos;

	private final OutputStream crcBypass = new OutputStream() {
		@Override
		public void write(int b) throws IOException {
			writeRawByte(b);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			writeRawBytes(b, off, len);
		}
	};

	public CRCStopBitOutputStream(OutputStream out, CRC crc) {
		this(out, crc, 1 << 15);
	}

	public CRCStopBitOutputStream(OutputStream out, CRC crc, int bufferSize) {
		super(out, crc);
		if (bufferSize < MIN_VARINT_BYTES) {
			throw new IllegalArgumentException("bufferSize must be >= " + MIN_VARINT_BYTES);
		}
		this.buf = new byte[bufferSize];
	}

	@Override
	public void writeCRC() throws IOException {
		crc.writeCRC(crcBypass);
		flushBuffer();
	}

	@Override
	public void write(int b) throws IOException {
		if (pos == buf.length) {
			flushBuffer();
		}
		buf[pos++] = (byte) b;
		crc.update((byte) b);
	}

	@Override
	public void write(byte[] b) throws IOException {
		write(b, 0, b.length);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		if (b == null) {
			throw new NullPointerException("b");
		}
		if ((off | len) < 0 || len > b.length - off) {
			throw new IndexOutOfBoundsException();
		}
		if (len == 0) {
			return;
		}

		crc.update(b, off, len);

		if (len >= buf.length) {
			flushBuffer();
			out.write(b, off, len);
			return;
		}

		if (len > buf.length - pos) {
			flushBuffer();
		}
		System.arraycopy(b, off, buf, pos, len);
		pos += len;
	}

	@Override
	public void flush() throws IOException {
		flushBuffer();
		out.flush();
	}

	@Override
	public void writeVByteLong(long value) throws IOException {
		if (value < 0) {
			throw new IllegalArgumentException("Only can encode VByte of positive values");
		}

		if (buf.length - pos < 9) {
			flushBuffer();
		}

		final int start = pos;
		int p = pos;

		if (value < 0x80) {
			// Byte 1 (0-7 bits)
			buf[p++] = (byte) (value | 0x80);
		} else {
			buf[p++] = (byte) (value & 0x7F);
			value >>>= 7;
			if (value < 0x80) {
				// Byte 2 (8-14 bits)
				buf[p++] = (byte) (value | 0x80);
			} else {
				buf[p++] = (byte) (value & 0x7F);
				value >>>= 7;
				if (value < 0x80) {
					// Byte 3 (15-21 bits)
					buf[p++] = (byte) (value | 0x80);
				} else {
					buf[p++] = (byte) (value & 0x7F);
					value >>>= 7;
					if (value < 0x80) {
						// Byte 4 (22-28 bits)
						buf[p++] = (byte) (value | 0x80);
					} else {
						buf[p++] = (byte) (value & 0x7F);
						value >>>= 7;
						if (value < 0x80) {
							// Byte 5 (29-35 bits)
							buf[p++] = (byte) (value | 0x80);
						} else {
							buf[p++] = (byte) (value & 0x7F);
							value >>>= 7;
							if (value < 0x80) {
								// Byte 6 (36-42 bits)
								buf[p++] = (byte) (value | 0x80);
							} else {
								buf[p++] = (byte) (value & 0x7F);
								value >>>= 7;
								if (value < 0x80) {
									// Byte 7 (43-49 bits)
									buf[p++] = (byte) (value | 0x80);
								} else {
									buf[p++] = (byte) (value & 0x7F);
									value >>>= 7;
									if (value < 0x80) {
										// Byte 8 (50-56 bits)
										buf[p++] = (byte) (value | 0x80);
									} else {
										// Byte 9 (57-63 bits)
										// A positive long uses at most 63 bits.
										// We have shifted 56 bits (8 * 7).
										// Remaining bits < 7.
										// Therefore, the 9th byte is always the
										// end.
										buf[p++] = (byte) (value & 0x7F);
										buf[p++] = (byte) ((value >>> 7) | 0x80);
									}
								}
							}
						}
					}
				}
			}
		}

		this.pos = p;
		updateCrcFromArray(buf, start, p - start);
	}

	private void flushBuffer() throws IOException {
		if (pos > 0) {
			out.write(buf, 0, pos);
			pos = 0;
		}
	}

	private void updateCrcFromArray(byte[] a, int off, int len) {
		if (len <= 0) {
			return;
		}
		if (len == 1) {
			crc.update(a[off]);
		} else {
			crc.update(a, off, len);
		}
	}

	private void writeRawByte(int b) throws IOException {
		if (pos == buf.length) {
			flushBuffer();
		}
		buf[pos++] = (byte) b;
	}

	private void writeRawBytes(byte[] b, int off, int len) throws IOException {
		if (b == null) {
			throw new NullPointerException("b");
		}
		if ((off | len) < 0 || len > b.length - off) {
			throw new IndexOutOfBoundsException();
		}
		if (len == 0) {
			return;
		}

		if (len >= buf.length) {
			flushBuffer();
			out.write(b, off, len);
			return;
		}

		if (len > buf.length - pos) {
			flushBuffer();
		}
		System.arraycopy(b, off, buf, pos, len);
		pos += len;
	}
}
