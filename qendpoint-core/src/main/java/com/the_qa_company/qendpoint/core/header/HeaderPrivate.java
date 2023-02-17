package com.the_qa_company.qendpoint.core.header;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.ControlInfo;

import java.io.IOException;
import java.io.InputStream;

public interface HeaderPrivate extends Header {
	void load(InputStream input, ControlInfo ci, ProgressListener listener) throws IOException;
}
