package com.the_qa_company.qendpoint.core.util.io;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.exceptions.CRCException;
import com.the_qa_company.qendpoint.core.util.crc.CRC;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Buffered {@link CRCInputStream} that supports fast stop-bit (VByte) decoding.
 * <p>
 * Key property: CRC is updated only for bytes actually returned as data. CRC
 * digest bytes are read through a bypass view (no CRC update), and can be
 * consumed from the same internal buffer.
 * </p>
 */
public final class CRCStopBitInputStream extends CRCInputStream implements VByte.FastInput {
	private static final int MAX_SHIFT = 56; // 9 bytes * 7 bits = 63 bits

	private final byte[] buf;
	private int pos;
	private int limit;

	private final InputStream crcBypass = new InputStream() {
		@Override
		public int read() throws IOException {
			return readRawByte();
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			return readRawBytes(b, off, len);
		}
	};

	public CRCStopBitInputStream(InputStream in, CRC crc) {
		this(in, crc, 1 << 15);
	}

	public CRCStopBitInputStream(InputStream in, CRC crc, int bufferSize) {
		super(in, crc);
		if (bufferSize < 9) {
			throw new IllegalArgumentException("bufferSize must be >= 9 (max varint length)");
		}
		this.buf = new byte[bufferSize];
	}

	@Override
	public boolean readCRCAndCheck() throws IOException {
		return crc.readAndCheck(crcBypass);
	}

	@Override
	public void assertCRC() throws IOException {
		if (!crc.readAndCheck(crcBypass)) {
			throw new CRCException("Invalid crc exception");
		}
	}

	private int refill() throws IOException {
		final int n = in.read(buf, 0, buf.length);
		if (n > 0) {
			pos = 0;
			limit = n;
		}
		return n;
	}

	private int readRawByte() throws IOException {
		if (pos >= limit) {
			if (refill() < 0) {
				return -1;
			}
		}
		return buf[pos++] & 0xFF;
	}

	private int readRawBytes(byte[] b, int off, int len) throws IOException {
		if (b == null) {
			throw new NullPointerException("b");
		}
		if ((off | len) < 0 || len > b.length - off) {
			throw new IndexOutOfBoundsException();
		}
		if (len == 0) {
			return 0;
		}

		int total = 0;

		int avail = limit - pos;
		if (avail > 0) {
			final int cnt = Math.min(avail, len);
			System.arraycopy(buf, pos, b, off, cnt);
			pos += cnt;
			off += cnt;
			len -= cnt;
			total += cnt;
			if (len == 0) {
				return total;
			}
		}

		if (len >= buf.length) {
			final int n = in.read(b, off, len);
			return (n < 0) ? (total == 0 ? -1 : total) : (total + n);
		}

		final int n = refill();
		if (n < 0) {
			return (total == 0 ? -1 : total);
		}

		final int cnt = Math.min(limit - pos, len);
		System.arraycopy(buf, pos, b, off, cnt);
		pos += cnt;
		total += cnt;
		return total;
	}

	@Override
	public int read() throws IOException {
		final int v = readRawByte();
		if (v >= 0) {
			crc.update((byte) v);
		}
		return v;
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		if (b == null) {
			throw new NullPointerException("b");
		}
		if ((off | len) < 0 || len > b.length - off) {
			throw new IndexOutOfBoundsException();
		}
		if (len == 0) {
			return 0;
		}

		int total = 0;

		int avail = limit - pos;
		if (avail > 0) {
			final int cnt = Math.min(avail, len);
			final int p = pos;
			System.arraycopy(buf, p, b, off, cnt);
			pos = p + cnt;

			updateCrcFromArray(buf, p, cnt);

			off += cnt;
			len -= cnt;
			total += cnt;
			if (len == 0) {
				return total;
			}
		}

		if (len >= buf.length) {
			final int n = in.read(b, off, len);
			if (n < 0) {
				return (total == 0 ? -1 : total);
			}
			if (n > 0) {
				crc.update(b, off, n);
			}
			return total + n;
		}

		final int n = refill();
		if (n < 0) {
			return (total == 0 ? -1 : total);
		}

		final int cnt = Math.min(limit - pos, len);
		final int p = pos;
		System.arraycopy(buf, p, b, off, cnt);
		pos = p + cnt;
		updateCrcFromArray(buf, p, cnt);

		total += cnt;
		return total;
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

	@Override
	public long readVByteLong() throws IOException {
		// Pull hot fields into locals (helps JIT keep them in registers).
		byte[] b = buf;
		int p = pos;
		int lim = limit;

		// Ensure at least one byte is available.
		if (p >= lim) {
			if (refill() < 0) {
				throw new EOFException();
			}
			b = buf;
			p = pos;
			lim = limit;
		}

		// Fast path: if we have at least 9 bytes, we can decode without any
		// refill checks.
		// (Max stop-bit varint length for 63 bits is 9 bytes.)
		if (lim - p >= 9) {
			final int start = p;

			int x = b[p++]; // signed; stop-bit set => x < 0
			long r = x & 0x7FL;
			if (x < 0) {
				pos = p;
				// Common case: 1-byte varint. Avoid reloading buf[start].
				crc.update((byte) x);
				return r;
			}

			x = b[p++];
			r |= (long) (x & 0x7F) << 7;
			if (x < 0) {
				pos = p;
				crc.update(b, start, 2);
				return r;
			}

			x = b[p++];
			r |= (long) (x & 0x7F) << 14;
			if (x < 0) {
				pos = p;
				crc.update(b, start, 3);
				return r;
			}

			x = b[p++];
			r |= (long) (x & 0x7F) << 21;
			if (x < 0) {
				pos = p;
				crc.update(b, start, 4);
				return r;
			}

			x = b[p++];
			r |= (long) (x & 0x7F) << 28;
			if (x < 0) {
				pos = p;
				crc.update(b, start, 5);
				return r;
			}

			x = b[p++];
			r |= (long) (x & 0x7F) << 35;
			if (x < 0) {
				pos = p;
				crc.update(b, start, 6);
				return r;
			}

			x = b[p++];
			r |= (long) (x & 0x7F) << 42;
			if (x < 0) {
				pos = p;
				crc.update(b, start, 7);
				return r;
			}

			x = b[p++];
			r |= (long) (x & 0x7F) << 49;
			if (x < 0) {
				pos = p;
				crc.update(b, start, 8);
				return r;
			}

			x = b[p++];
			r |= (long) (x & 0x7F) << 56;
			if (x < 0) {
				pos = p;
				crc.update(b, start, 9);
				return r;
			}

			// Malformed: more than 9 bytes.
			// Keep behavior aligned with the old method (exception path is
			// cold).
			throw new IllegalArgumentException("Malformed stop-bit varint: more than 9 bytes");
		}

		// General path (rare with decent buffer sizes): may cross buffer
		// boundaries.
		long r = 0L;
		int shift = 0;

		while (true) {
			final int start = p;

			while (p < lim) {
				final int x = b[p++]; // signed
				r |= (long) (x & 0x7F) << shift;

				if (x < 0) {
					pos = p;
					updateCrcFromArray(b, start, p - start);
					return r;
				}

				if ((shift += 7) > MAX_SHIFT) {
					// Match original behavior: CRC for the current chunk is not
					// updated on malformed varint.
					pos = p;
					throw new IllegalArgumentException("Malformed stop-bit varint: more than 9 bytes");
				}
			}

			// Consumed the whole remaining buffer chunk without seeing a
			// stop-bit.
			updateCrcFromArray(b, start, p - start);

			// Important for EOF behavior/state consistency: we've consumed up
			// to p (== lim).
			pos = p;

			if (refill() < 0) {
				throw new EOFException();
			}

			b = buf;
			p = pos;
			lim = limit;
		}
	}
}
