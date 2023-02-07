package com.the_qa_company.qendpoint.core.header;

import java.io.IOException;
import java.io.InputStream;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.ControlInfo;

public interface HeaderPrivate extends Header {
	void load(InputStream input, ControlInfo ci, ProgressListener listener) throws IOException;
}
