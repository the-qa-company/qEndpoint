package com.the_qa_company.qendpoint.core.rdf.parsers;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TurtleChunkerDirectiveTest {

	@Test
	public void acceptsLeadingCommentAndWhitespaceBeforePrefix() {
		String ttl = "# header\n   @prefix ex: <http://example.com/> .\n" + "<http://ex/s> <http://ex/p> \"o\" .";
		InputStream input = new ByteArrayInputStream(ttl.getBytes(StandardCharsets.UTF_8));

		TurtleChunker chunker = new TurtleChunker(input);
		TurtleChunker.BlockIterator iterator = chunker.blockIterator();
		List<String> prefixes = new ArrayList<>();
		iterator.setPrefixConsumer(prefixes::add);

		assertTrue("Expected statement block", iterator.hasNext());
		String block = iterator.next();
		assertEquals("<http://ex/s> <http://ex/p> \"o\" .", block);
		assertFalse("Expected single statement block", iterator.hasNext());
		assertEquals("Expected one prefix block", 1, prefixes.size());
		assertEquals("# header\n   @prefix ex: <http://example.com/> .", prefixes.get(0));
	}
}
