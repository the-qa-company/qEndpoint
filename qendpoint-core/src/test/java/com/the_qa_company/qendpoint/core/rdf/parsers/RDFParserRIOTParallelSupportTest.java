package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import org.junit.Test;

import java.io.InputStream;
import java.lang.reflect.Method;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class RDFParserRIOTParallelSupportTest {
	@Test
	public void concurrentInputStreamClassesExist() throws Exception {
		assertClassExists("com.the_qa_company.qendpoint.core.rdf.parsers.ConcurrentInputStream");
		assertClassExists("com.the_qa_company.qendpoint.core.rdf.parsers.ChunkedConcurrentInputStream");
		assertClassExists("com.the_qa_company.qendpoint.core.rdf.parsers.TurtleChunker");
	}

	@Test
	public void riotParserSupportsParallelDoParse() throws Exception {
		Class<?> cls = Class.forName("com.the_qa_company.qendpoint.core.rdf.parsers.RDFParserRIOT");
		Method method;
		try {
			method = cls.getMethod("doParse", InputStream.class, String.class, RDFNotation.class, boolean.class,
					RDFParserCallback.RDFCallback.class, boolean.class);
		} catch (NoSuchMethodException e) {
			fail("Missing parallel doParse overload on RDFParserRIOT");
			return;
		}
		assertNotNull(method);
	}

	private static void assertClassExists(String name) {
		try {
			Class.forName(name);
		} catch (ClassNotFoundException e) {
			fail("Missing class " + name);
		}
	}
}
