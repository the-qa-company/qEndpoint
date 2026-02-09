package com.the_qa_company.qendpoint.core.util.string;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.util.io.BigByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.BigMappedByteBuffer;

/**
 * CharSequence implementation suitable for appending or replacing the suffix of
 * the string. It grows as necessary but it never returns that size back.
 */
public final class ReplazableString implements CharSequence, ByteString {

	// Keep fields package-visible as in your original (in case other code
	// relies on it)
	byte[] buffer;
	int used;

	public ReplazableString() {
		this(128);
	}

	public ReplazableString(int initialCapacity) {
		buffer = new byte[Math.max(1, initialCapacity)];
		used = 0;
	}

	public ReplazableString(byte[] buffer) {
		this.buffer = buffer;
		this.used = buffer.length;
	}

	@Override
	public byte[] getBuffer() {
		return buffer;
	}

	public void clear() {
		used = 0;
	}

	/**
	 * Like your ensureSize, but returns the (possibly reallocated) buffer for
	 * local caching.
	 */
	private byte[] ensureCapacity(int minCapacity) {
		byte[] buf = this.buffer;
		if (minCapacity > buf.length) {
			// Keep your growth policy (2x) to preserve memory/perf
			// characteristics
			int newCap = Math.max(minCapacity, buf.length * 2);
			buf = Arrays.copyOf(buf, newCap);
			this.buffer = buf;
		}
		return buf;
	}

	public void append(byte c) {
		int u = used;
		byte[] buf = ensureCapacity(u + 1);
		buf[u] = c;
		used = u + 1;
	}

	public void append(byte[] data) {
		append(data, 0, data.length);
	}

	public void append(byte[] data, int offset, int len) {
		if (len <= 0) {
			return;
		}
		int u = used;
		byte[] buf = ensureCapacity(u + len);
		System.arraycopy(data, offset, buf, u, len);
		used = u + len;
	}

	public void append(BigByteBuffer data, long offset, int len) {
		if (len <= 0) {
			return;
		}
		int u = used;
		byte[] buf = ensureCapacity(u + len);
		data.get(buf, offset, u, len);
		used = u + len;
	}

	public void append(CharSequence other) {
		other = DelayedString.unwrap(other);

		// Common fast path: if it is already a ByteString, don't charAt() it
		// byte-by-byte
		if (other instanceof ByteString bs) {
			append(bs.getBuffer(), 0, bs.length());
			return;
		}

		final int n = other.length();
		if (n <= 0) {
			return;
		}
		int u = used;
		byte[] buf = ensureCapacity(u + n);
		for (int i = 0; i < n; i++) {
			buf[u + i] = (byte) other.charAt(i);
		}
		used = u + n;
	}

	public void appendNoCompact(CharSequence other) {
		other = DelayedString.unwrap(other);

		if (other instanceof ByteString) {
			appendNoCompact((ByteString) other);
		} else {
			append(other.toString().getBytes(ByteStringUtil.STRING_ENCODING));
		}
	}

	public void appendNoCompact(ByteString other) {
		append(other.getBuffer(), 0, other.length());
	}

	public void appendNoCompact(CharSequence other, int offset, int length) {
		other = DelayedString.unwrap(other);

		if (other instanceof ByteString) {
			append(((ByteString) other).getBuffer(), offset, length);
		} else {
			// This still allocates; if this is hot, see notes below for a
			// non-substring encoder approach.
			append(other.toString().substring(offset, offset + length).getBytes(ByteStringUtil.STRING_ENCODING));
		}
	}

	public void replace(ByteString other) {
		final int n = other.length();
		byte[] buf = ensureCapacity(n);
		System.arraycopy(other.getBuffer(), 0, buf, 0, n);
		used = n;
	}

	public void replace(CharSequence other) {
		other = DelayedString.unwrap(other);

		if (other instanceof ByteString) {
			replace((ByteString) other);
		} else {
			used = 0;
			byte[] bytes = other.toString().getBytes(StandardCharsets.UTF_8);
			replace(0, bytes, 0, bytes.length);
		}
	}

	public void replace(int pos, byte[] data, int offset, int len) {
		byte[] buf = ensureCapacity(pos + len);
		System.arraycopy(data, offset, buf, pos, len);
		used = pos + len;
	}

	public void replace(int pos, BigByteBuffer data, long offset, int len) {
		byte[] buf = ensureCapacity(pos + len);
		data.get(buf, offset, pos, len);
		used = pos + len;
	}

	public void replace(InputStream in, int pos, int len) throws IOException {
		if (len == 0) {
			used = pos;
			return;
		}
		byte[] buf = ensureCapacity(pos + len);
		int read = 0;
		while (read < len) {
			int n = in.read(buf, pos + read, len - read);
			if (n < 0) {
				throw new EOFException("EOF while reading array from InputStream");
			}
			read += n;
		}
		used = pos + len;
	}

	public void replace(ByteBuffer in, int pos, int len) throws IOException {
		byte[] buf = ensureCapacity(pos + len);
		in.get(buf, pos, len);
		used = pos + len;
	}

	public void replace(BigMappedByteBuffer in, int pos, int len) throws IOException {
		byte[] buf = ensureCapacity(pos + len);
		in.get(buf, pos, len);
		used = pos + len;
	}

	// ---------------------------
	// Faster null-terminated reads
	// ---------------------------

	/**
	 * Scan a region of a byte[] for 0. Returns index relative to off, or -1 if
	 * none. This is a simple loop; for many workloads the I/O dominates anyway.
	 * If profiling says the scan itself is hot, you can replace this with a
	 * long-word scanner (memchr-style) later.
	 */
	private static int indexOfZero(byte[] a, int off, int len) {
		final int end = off + len;
		for (int i = off; i < end; i++) {
			if (a[i] == 0) {
				return i - off;
			}
		}
		return -1;
	}

	private static void skipFully(InputStream in, long n) throws IOException {
		long remaining = n;
		while (remaining > 0) {
			long skipped = in.skip(remaining);
			if (skipped > 0) {
				remaining -= skipped;
				continue;
			}
			// skip() is allowed to return 0; force progress
			if (in.read() == -1) {
				throw new EOFException("EOF while skipping " + n + " bytes");
			}
			remaining--;
		}
	}

	private static final int READ_AHEAD = 8192;

	/**
	 * Null-terminated string from InputStream. Key speed fix: - If this is a
	 * FileInputStream, use FileChannel to read blocks + seek back. That avoids
	 * the pathological read()-per-byte slow path.
	 */
	public void replace(InputStream in, int pos) throws IOException {
		// Fast path for the most common "markUnsupported" stream in the real
		// world:
		// FileInputStream. We can over-read and then seek back to the correct
		// position.
		if (in instanceof FileInputStream fis) {
			replaceFromFileChannel(fis.getChannel(), pos);
			return;
		}

		if (!in.markSupported()) {
			// Semantics constraint: for non-seekable streams, you cannot safely
			// over-read and “unread”.
			// So this remains byte-by-byte.
			replace2(in, pos);
			return;
		}

		int u = pos;
		byte[] buf = this.buffer;

		while (true) {
			buf = ensureCapacity(u + READ_AHEAD);
			in.mark(READ_AHEAD);
			int numRead = in.read(buf, u, READ_AHEAD);
			if (numRead == -1) {
				this.buffer = buf;
				this.used = u;
				throw new IllegalArgumentException(
						"Was reading a string but stream ended before finding the null terminator");
			}

			int idx = indexOfZero(buf, u, numRead);
			if (idx >= 0) {
				in.reset();
				skipFully(in, idx + 1L); // move stream to after the terminator
				u += idx;
				this.buffer = buf;
				this.used = u;
				return;
			}

			u += numRead;
		}
	}

	private void replaceFromFileChannel(FileChannel ch, int pos) throws IOException {
		int u = pos;
		byte[] buf = this.buffer;

		final long startPos = ch.position();
		long consumed = 0;

		while (true) {
			buf = ensureCapacity(u + READ_AHEAD);

			// Read up to READ_AHEAD bytes directly into our internal buffer.
			ByteBuffer dst = ByteBuffer.wrap(buf, u, READ_AHEAD);
			int n = ch.read(dst);
			if (n == -1) {
				this.buffer = buf;
				this.used = u;
				throw new IllegalArgumentException(
						"Was reading a string but stream ended before finding the null terminator");
			}
			if (n == 0) {
				// Very unusual for FileChannel, but avoid infinite loop
				continue;
			}

			int idx = indexOfZero(buf, u, n);
			if (idx >= 0) {
				u += idx;
				// Seek back to position right after the terminator, undoing any
				// over-read.
				ch.position(startPos + consumed + idx + 1L);
				this.buffer = buf;
				this.used = u;
				return;
			}

			u += n;
			consumed += n;
		}
	}

	/**
	 * Null-terminated string from ByteBuffer, but done as "scan + bulk copy". -
	 * Heap ByteBuffers: direct array scan + System.arraycopy - Direct
	 * ByteBuffers: duplicate() + bulk get()
	 */
	public void replace(ByteBuffer in, int pos) throws IOException {
		final int start = in.position();
		final int rem = in.remaining();
		if (rem <= 0) {
			throw new IllegalArgumentException(
					"Was reading a string but buffer ended before finding the null terminator");
		}

		if (in.hasArray()) {
			final byte[] src = in.array();
			final int srcPos = in.arrayOffset() + start;
			final int idx = indexOfZero(src, srcPos, rem);
			if (idx < 0) {
				throw new IllegalArgumentException(
						"Was reading a string but buffer ended before finding the null terminator");
			}
			replace(pos, src, srcPos, idx);
			in.position(start + idx + 1);
			return;
		}

		// Direct / read-only buffer: scan by absolute get(), then bulk copy
		// with a duplicate slice
		int zeroAt = -1;
		for (int i = start; i < start + rem; i++) {
			if (in.get(i) == 0) {
				zeroAt = i;
				break;
			}
		}
		if (zeroAt < 0) {
			throw new IllegalArgumentException(
					"Was reading a string but buffer ended before finding the null terminator");
		}

		final int len = zeroAt - start;
		ensureCapacity(pos + len);

		ByteBuffer dup = in.duplicate();
		dup.position(start).limit(zeroAt);
		dup.get(this.buffer, pos, len);

		this.used = pos + len;
		in.position(zeroAt + 1);
	}

	// Original "slow but safe" InputStream non-mark path
	public void replace2(InputStream in, int pos) throws IOException {
		int u = pos;

		int length = buffer.length;
		byte[] buf = this.buffer;

		while (true) {
			int value = in.read();
			if (value == -1) {
				this.used = u;
				this.buffer = buf;
				throw new IllegalArgumentException(
						"Was reading a string but stream ended before finding the null terminator");
			}
			if (value == 0) {
				break;
			}
			if (u >= length) {
				buf = Arrays.copyOf(buf, length * 2);
				length = buf.length;
			}
			buf[u++] = (byte) (value & 0xFF);
		}
		this.used = u;
		this.buffer = buf;
	}

	public int replace2(BigMappedByteBuffer buffer, long offset, int pos) {
		int u = pos;
		byte[] buf = this.buffer;
		int cap = buf.length;

		int shift = 0;
		while (true) {
			int value = buffer.get(offset + shift++);
			if (value == 0) {
				break;
			}
			if (u >= cap) {
				buf = Arrays.copyOf(buf, cap * 2);
				cap = buf.length;
			}
			buf[u++] = (byte) (value & 0xFF);
		}

		this.used = u;
		this.buffer = buf;
		return shift;
	}

	public int replace2(BigByteBuffer buffer, long offset, int pos) {
		int u = pos;
		byte[] buf = this.buffer;
		int cap = buf.length;

		int shift = 0;
		while (true) {
			int value = buffer.get(offset + shift++);
			if (value == 0) {
				break;
			}
			if (u >= cap) {
				buf = Arrays.copyOf(buf, cap * 2);
				cap = buf.length;
			}
			buf[u++] = (byte) (value & 0xFF);
		}

		this.used = u;
		this.buffer = buf;
		return shift;
	}

	public void replace(BigMappedByteBuffer in, int pos) throws IOException {
		int u = pos;
		byte[] buf = this.buffer;
		int cap = buf.length;

		long n = in.capacity() - in.position();
		while (n-- != 0) {
			byte value = in.get();
			if (value == 0) {
				this.used = u;
				this.buffer = buf;
				return;
			}
			if (u >= cap) {
				buf = Arrays.copyOf(buf, cap * 2);
				cap = buf.length;
			}
			buf[u++] = value;
		}
		this.used = u;
		this.buffer = buf;
		throw new IllegalArgumentException("Was reading a string but stream ended before finding the null terminator");
	}

	@Override
	public char charAt(int index) {
		if (index >= used) {
			throw new StringIndexOutOfBoundsException("Invalid index " + index + " length " + length());
		}
		return (char) (buffer[index] & 0xFF);
	}

	@Override
	public int length() {
		return used;
	}

	@Override
	public int hashCode() {
		// FNV Hash function: http://isthe.com/chongo/tech/comp/fnv/
		int hash = (int) 2166136261L;
		int i = used;
		byte[] buf = buffer;

		while (i-- != 0) {
			hash = (hash * 16777619) ^ buf[i];
		}

		return hash;
	}

	@Override
	public boolean equals(Object o) {
		if (o == null) {
			return false;
		}
		if (this == o) {
			return true;
		}
		if (o instanceof CompactString cmp) {
			if (this.used != cmp.data.length) {
				return false;
			}
			// Use JDK's optimized range equals
			return Arrays.equals(this.buffer, 0, this.used, cmp.data, 0, this.used);
		} else if (o instanceof ReplazableString cmp) {
			if (this.used != cmp.used) {
				return false;
			}
			return Arrays.equals(this.buffer, 0, this.used, cmp.buffer, 0, this.used);
		} else if (o instanceof CharSequence other) {
			return length() == other.length() && CharSequenceComparator.getInstance().compare(this, other) == 0;
		}
		throw new NotImplementedException();
	}

	@Override
	public int compareTo(ByteString other) {
		if (this == other) {
			return 0;
		}

		final int len1 = this.used;
		final int len2 = other.length();
		final int n = Math.min(len1, len2);

		final byte[] a = this.buffer;
		final byte[] b = other.getBuffer();

		final int mismatch = Arrays.mismatch(a, 0, n, b, 0, n);
		if (mismatch >= 0) {
			return (a[mismatch] & 0xFF) - (b[mismatch] & 0xFF);
		}
		return len1 - len2;
	}

	@Override
	public ByteString subSequence(int start, int end) {
		if (start < 0 || end > (this.length()) || (end - start) < 0) {
			throw new IllegalArgumentException(
					"Illegal range " + start + "-" + end + " for sequence of length " + length());
		}
		byte[] newdata = new byte[end - start];
		System.arraycopy(buffer, start, newdata, 0, end - start);
		return new ReplazableString(newdata);
	}

	@Override
	public String toString() {
		return new String(buffer, 0, used, ByteStringUtil.STRING_ENCODING);
	}

	public CharSequence getDelayed() {
		return new DelayedString(this);
	}

	public void copy(ReplazableString prev) {
		if (prev.buffer.length > buffer.length) {
			buffer = Arrays.copyOf(prev.buffer, prev.buffer.length);
		} else {
			System.arraycopy(prev.buffer, 0, buffer, 0, prev.used);
		}
		used = prev.used;
	}
}
