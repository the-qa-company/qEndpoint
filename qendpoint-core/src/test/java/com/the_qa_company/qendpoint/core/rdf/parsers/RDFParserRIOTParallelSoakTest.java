package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback.RDFCallback;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class RDFParserRIOTParallelSoakTest {
	@Test
	public void parallelParseProcessesAllTriples() throws Exception {
		int total = 20000;
		String data = buildDataset(total);

		RDFParserRIOT parser = new RDFParserRIOT();
		ConcurrentHashMap<String, Boolean> seen = new ConcurrentHashMap<>();
		AtomicInteger seenCount = new AtomicInteger();
		RDFCallback callback = (triple, pos) -> {
			String normalized = triple.toString();
			if (seen.putIfAbsent(normalized, Boolean.TRUE) == null) {
				seenCount.incrementAndGet();
			}
		};

		parser.doParse(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)), "http://example.org/",
				RDFNotation.NTRIPLES, true, callback, true);

		assertEquals(total, seenCount.get());
		assertEquals(total, seen.size());
	}

	private static String buildDataset(int total) {
		StringBuilder builder = new StringBuilder(total * 48);
		for (int i = 0; i < total; i++) {
			String subject = (i % 5 == 0) ? "_:b" + i : "<http://example.org/s" + i + '>';
			builder.append(subject).append(" <http://example.org/p> \"o").append(i).append("\" .\n");
		}
		return builder.toString();
	}
}
