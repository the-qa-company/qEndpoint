package com.the_qa_company.qendpoint.core.dictionary;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilCat.CatMapping;
import com.the_qa_company.qendpoint.core.dictionary.impl.utilCat.CatMappingBack;
import com.the_qa_company.qendpoint.core.util.string.ByteString;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

public interface DictionaryCat extends Closeable {
	void cat(Dictionary dictionary1, Dictionary dictionary2, ProgressListener listener) throws IOException;

	CatMappingBack getMappingS();

	long getNumShared();

	Map<ByteString, CatMapping> getAllMappings();
}
