package com.the_qa_company.qendpoint.core.util.string;

import java.util.Objects;

public class TypedLiteralCompactString implements ByteString {
	private ByteString value;
	private ByteString type;
	byte[] buffer;
	private int hash;

	public TypedLiteralCompactString(ByteString value, ByteString type) {
		this.value = value;
		this.type = type;
	}

	public void setValue(ByteString value, ByteString type) {
		this.value = value;
		this.type = type;
		buffer = null;
		hash = 0;
	}

	public void setType(ByteString type) {
		if (this.type != type) {
			// only for null values?
			buffer = null;
			hash = 0;
		}
		this.type = type;
	}

	private void computeBuffer() {
		if (buffer != null) {
			return;
		}

		int len = 2 + value.length();

		if (type != null) {
			len += 2 + type.length();
		}

		buffer = new byte[len];
		buffer[0] = '"';
		int vl = value.length();
		System.arraycopy(value.getBuffer(), 0, buffer, 1, vl);
		buffer[vl + 1] = '"';

		if (type != null) {
			buffer[vl + 2] = '^';
			buffer[vl + 3] = '^';

			System.arraycopy(type.getBuffer(), 0, buffer, vl + 4, type.length());
		}
	}

	@Override
	public byte[] getBuffer() {
		computeBuffer();
		return buffer;
	}

	@Override
	public int length() {
		if (buffer != null)
			return buffer.length;
		if (type != null) {
			return 1 + value.length() + 1 + 2 + type.length();
		}
		return 2 + value.length();
	}

	@Override
	public char charAt(int index) {
		if (buffer != null)
			return (char) (buffer[index] & 0xFF);

		if (index == 0) {
			return '"';
		}
		int vlen = value.length();
		if (index <= vlen) {
			return value.charAt(index - 1);
		}

		if (index == vlen + 1) {
			return '"';
		}

		if (type == null) {
			throw new IllegalArgumentException("no type");
		}

		if (index == vlen + 2 || index == vlen + 3) {
			return '^';
		}

		return type.charAt(index - vlen - 4);
	}

	@Override
	public ByteString subSequence(int start, int end) {
		computeBuffer();
		if (start < 0 || end > (this.length()) || (end - start) < 0) {
			throw new IllegalArgumentException(
					"Illegal range " + start + "-" + end + " for sequence of length " + length());
		}
		byte[] newdata = new byte[end - start];
		System.arraycopy(buffer, start, newdata, 0, end - start);
		return new CompactString(newdata);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof ByteString cs)) {
			return false;
		}
		if (obj instanceof TypedLiteralCompactString tlc) {
			if (Objects.equals(tlc.type, type) && tlc.value.equals(value)) {
				return true;
			}
		}
		return compareTo(cs) == 0;
	}

	@Override
	public int hashCode() {
		computeBuffer();

		if (hash == 0) {
			hash = (int) 2166136261L;
			int i = buffer.length;

			while (i-- != 0) {
				hash = (hash * 16777619) ^ buffer[i];
			}
		}
		return hash;
	}

	@Override
	public String toString() {
		computeBuffer();
		return new String(buffer, ByteStringUtil.STRING_ENCODING);
	}

	@Override
	public int compareTo(ByteString other) {
		if (other instanceof TypedLiteralCompactString tlcs) {
			int c = value.compareTo(tlcs.value);

			if (c != 0) {
				return c;
			}

			if (type == null) {
				if (tlcs.type == null) {
					return 0; // same
				}

				// we don't have a type, but they do, so they're after
				return -1;
			}

			if (tlcs.type == null) {
				// we have a type, but they don't, so we're after
				return 1;
			}

			return type.compareTo(tlcs.type);
		}
		return ByteString.super.compareTo(other);
	}

	public ByteString getValue() {
		return value;
	}

	public ByteString getType() {
		return type;
	}
}
