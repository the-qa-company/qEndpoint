package com.the_qa_company.qendpoint.core.util.string;

public interface NumberByteString extends ByteString {
	enum NumberPriority {
		LONG, DOUBLE, DECIMAL;

		boolean isBetter(NumberPriority p2) {
			return ordinal() < p2.ordinal();
		}
	}

	boolean isSameNumber(NumberByteString other);

	NumberPriority numberPriority();

	default ByteString asByteString() {
		byte[] buffer = getBuffer();
		byte[] b2 = new byte[length()];
		System.arraycopy(buffer, 0, b2, 0, b2.length);

		return new CompactString(b2);
	}
}
