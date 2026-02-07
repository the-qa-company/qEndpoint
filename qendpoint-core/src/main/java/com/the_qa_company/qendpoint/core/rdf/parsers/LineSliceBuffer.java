package com.the_qa_company.qendpoint.core.rdf.parsers;

import java.util.Arrays;

final class LineSliceBuffer {
	private final int[] lineStarts;
	private final int[] lineEnds;

	private char[] slab;
	private int slabUsed;
	private int lineCount;

	LineSliceBuffer(int maxLines, int initialCapacity) {
		this.lineStarts = new int[maxLines];
		this.lineEnds = new int[maxLines];
		this.slab = new char[Math.max(64, initialCapacity)];
	}

	void reset() {
		lineCount = 0;
		slabUsed = 0;
	}

	int count() {
		return lineCount;
	}

	boolean isEmpty() {
		return lineCount == 0;
	}

	char[] slab() {
		return slab;
	}

	int startAt(int index) {
		return lineStarts[index];
	}

	int endAt(int index) {
		return lineEnds[index];
	}

	int lineStart() {
		return slabUsed;
	}

	void appendSegment(char[] src, int offset, int length) {
		if (length == 0) {
			return;
		}
		ensureCapacity(length);
		System.arraycopy(src, offset, slab, slabUsed, length);
		slabUsed += length;
	}

	int finishLine(int lineStart) {
		int len = slabUsed - lineStart;
		if (len > 0) {
			record(lineStart, slabUsed);
		}
		return len;
	}

	private void ensureCapacity(int segmentLen) {
		if (slabUsed + segmentLen <= slab.length) {
			return;
		}
		int targetSize = Math.max(slab.length * 2, slabUsed + segmentLen);
		slab = Arrays.copyOf(slab, targetSize);
	}

	private void record(int start, int end) {
		lineStarts[lineCount] = start;
		lineEnds[lineCount] = end;
		lineCount++;
	}
}
