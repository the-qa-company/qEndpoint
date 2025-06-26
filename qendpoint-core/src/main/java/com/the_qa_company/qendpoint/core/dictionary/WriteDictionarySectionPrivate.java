package com.the_qa_company.qendpoint.core.dictionary;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;

import java.io.IOException;

public interface WriteDictionarySectionPrivate extends DictionarySectionPrivate {
	WriteDictionarySectionPrivateAppender createAppender(long size, ProgressListener listener) throws IOException;
}
