package com.the_qa_company.qendpoint.core.dictionary;

import com.the_qa_company.qendpoint.core.util.string.ByteString;

import java.io.Closeable;
import java.io.IOException;

public interface WriteDictionarySectionPrivateAppender extends Closeable {
	void append(ByteString str) throws IOException;

	long getNumberElements();
}
