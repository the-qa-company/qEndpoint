package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.junit.Assert.assertNotNull;

public class RDFParserRIOTParallelSupportTest {
	@Test
	public void concurrentInputStreamClassesExist() {
		assertNotNull(ConcurrentInputStream.class);
		assertNotNull(ChunkedConcurrentInputStream.class);
		assertNotNull(TurtleChunker.class);
	}

	@Test
	public void riotParserSupportsParallelDoParse() throws Exception {
		RDFParserRIOT parser = new RDFParserRIOT();
		InputStream input = new ByteArrayInputStream(new byte[0]);
		RDFParserCallback.RDFCallback callback = (triple, pos) -> {};
		parser.doParse(input, "http://example.org/base", RDFNotation.NTRIPLES, true, callback, true);
	}
}
