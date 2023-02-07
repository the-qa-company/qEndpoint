package com.the_qa_company.qendpoint.core.util.string;

import java.util.stream.IntStream;

/**
 * CharSequence wrapper throwing an {@link java.lang.AssertionError} if we try
 * to read before the minimum index
 */
public class AssertionCharSequence implements ByteString {
	private final ByteString sequence;
	private final int minimumRead;

	/**
	 * create an assertion cs
	 *
	 * @param sequence    wrapped sequence
	 * @param minimumRead minimum index to read (inclusive)
	 */
	public AssertionCharSequence(ByteString sequence, int minimumRead) {
		this.sequence = sequence;
		this.minimumRead = minimumRead;
	}

	/**
	 * create an assertion cs
	 *
	 * @param sequence    wrapped sequence
	 * @param minimumRead minimum index to read (inclusive)
	 */
	public AssertionCharSequence(CharSequence sequence, int minimumRead) {
		this(ByteString.of(sequence), minimumRead);
	}

	@Override
	public int length() {
		return sequence.length();
	}

	@Override
	public char charAt(int index) {
		if (index < minimumRead) {
			throw new AssertionError("Tried to read before minimum index! " + index + " / " + minimumRead);
		}
		return sequence.charAt(index);
	}

	@Override
	public ByteString subSequence(int start, int end) {
		if (start < minimumRead) {
			throw new AssertionError(
					"Tried to create subSequence before minimum index! " + start + " / " + minimumRead);
		}
		return sequence.subSequence(start, end);
	}

	@Override
	public String toString() {
		throw new AssertionError("Tried to convert an AssertionCharSequence to string!");
	}

	@Override
	public IntStream chars() {
		return sequence.chars();
	}

	@Override
	public IntStream codePoints() {
		return sequence.codePoints();
	}

	public ByteString getSequence() {
		return sequence;
	}

	@Override
	public byte[] getBuffer() {
		return sequence.getBuffer();
	}
}
