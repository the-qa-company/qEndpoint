package com.the_qa_company.qendpoint.core.search;

import com.the_qa_company.qendpoint.core.hdt.HDTFactory;
import com.the_qa_company.qendpoint.core.search.component.HDTComponent;
import com.the_qa_company.qendpoint.core.search.component.HDTComponentTriple;
import com.the_qa_company.qendpoint.core.util.io.AbstractMapMemoryTest;

import java.io.IOException;

public class AbstractQueryTest extends AbstractMapMemoryTest {
	protected static final HDTQueryToolFactoryImpl FACTORY;
	protected static final HDTQueryTool MOCK_FACTORY;

	static {
		FACTORY = (HDTQueryToolFactoryImpl) HDTQueryToolFactory.getFactories().stream()
				.filter(factory -> factory instanceof HDTQueryToolFactoryImpl).findAny()
				.orElseThrow(() -> new AssertionError("Can't find " + HDTQueryToolFactoryImpl.class));
		try {
			MOCK_FACTORY = FACTORY.newGenericQueryTool(HDTFactory.createHDT());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	protected static HDTComponentTriple triple(String s, String p, String o) {
		return MOCK_FACTORY.triple(s, p, o);
	}

	protected static HDTComponent component(String s) {
		return MOCK_FACTORY.component(s);
	}
}
