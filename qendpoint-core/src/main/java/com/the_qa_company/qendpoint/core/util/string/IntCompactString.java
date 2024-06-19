package com.the_qa_company.qendpoint.core.util.string;

import java.math.BigDecimal;

public class IntCompactString implements NumberByteString {
	private long value;
	byte[] buffer = null;
	private int hash = 0;

	public IntCompactString(long value) {
		this.value = value;
	}

	private void computeBuffer() {
		if (buffer != null) {
			return;
		}

		buffer = String.valueOf(value).getBytes(ByteStringUtil.STRING_ENCODING);
	}

	public void setValue(long value) {
		if (value == this.value) {
			return;
		}
		// set new value and clear buffer
		this.value = value;
		buffer = null;
		hash = 0;
	}

	@Override
	public String toString() {
		computeBuffer();
		return new String(buffer, ByteStringUtil.STRING_ENCODING);
	}

	@Override
	public int hashCode() {
		// FNV Hash function: http://isthe.com/chongo/tech/comp/fnv/
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
	public int compareTo(ByteString other) {
		if (other instanceof NumberByteString ics) {
			if (numberPriority().isBetter(ics.numberPriority())) {
				return ics.compareTo(this);
			}
			return Long.compare(value, ics.longValue());
		}
		return NumberByteString.super.compareTo(other);
	}

	@Override
	public byte[] getBuffer() {
		computeBuffer();
		return buffer;
	}

	@Override
	public int length() {
		computeBuffer();
		return buffer.length;
	}

	@Override
	public char charAt(int index) {
		computeBuffer();
		return (char) (buffer[index] & 0xff);
	}

	@Override
	public ByteString copy() {
		return new IntCompactString(value);
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof ByteString bs)) {
			return false;
		}
		if (this == obj) {
			return true;
		}
		if (bs instanceof NumberByteString ics) {
			if (numberPriority().isBetter(ics.numberPriority())) {
				return ics.isSameNumber(this);
			}
			return isSameNumber(ics);
		}

		if (length() != bs.length()) {
			return false;
		}

		return compareTo(bs) == 0;
	}

	@Override
	public ByteString subSequence(int start, int end) {
		if (start < 0 || end > (this.length()) || (end - start) < 0) {
			throw new IllegalArgumentException(
					"Illegal range " + start + "-" + end + " for sequence of length " + length());
		}
		byte[] newdata = new byte[end - start];
		System.arraycopy(buffer, start, newdata, 0, end - start);
		return new CompactString(newdata);
	}

	@Override
	public long longValue() {
		return value;
	}

	@Override
	public BigDecimal decimalValue() {
		return new BigDecimal(value);
	}

	@Override
	public double doubleValue() {
		return value;
	}

	@Override
	public boolean isSameNumber(NumberByteString other) {
		return other.longValue() == value;
	}

	@Override
	public NumberPriority numberPriority() {
		return NumberPriority.LONG;
	}
}
