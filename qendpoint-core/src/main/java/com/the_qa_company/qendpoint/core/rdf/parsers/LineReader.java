package com.the_qa_company.qendpoint.core.rdf.parsers;

import java.io.IOException;
import java.io.Reader;

final class LineReader {
	private static final int DEFAULT_READ_BUFFER = 8192;

	private final char[] readBuffer = new char[DEFAULT_READ_BUFFER];
	private int readPos;
	private int readLimit;

	int readLine(Reader reader, LineSliceBuffer target) throws IOException {
		int lineStart = target.lineStart();

		while (true) {
			if (readPos >= readLimit) {
				int read = reader.read(readBuffer);
				if (read == -1) {
					int len = target.finishLine(lineStart);
					return len > 0 ? len : -1;
				}
				readPos = 0;
				readLimit = read;
			}

			int i = readPos;
			while (i < readLimit) {
				char c = readBuffer[i];
				if (c == '\n' || c == '\r') {
					int segmentLen = i - readPos;
					target.appendSegment(readBuffer, readPos, segmentLen);
					readPos = i + 1;

					if (c == '\r') {
						consumeLfIfPresent(reader);
					}

					return target.finishLine(lineStart);
				}
				i++;
			}

			int segmentLen = readLimit - readPos;
			if (segmentLen > 0) {
				target.appendSegment(readBuffer, readPos, segmentLen);
				readPos = readLimit;
			}
		}
	}

	private void consumeLfIfPresent(Reader reader) throws IOException {
		if (readPos >= readLimit) {
			int read = reader.read(readBuffer);
			if (read == -1) {
				readPos = 0;
				readLimit = 0;
				return;
			}
			readPos = 0;
			readLimit = read;
		}
		if (readPos < readLimit && readBuffer[readPos] == '\n') {
			readPos++;
		}
	}
}
