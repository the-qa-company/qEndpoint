package com.the_qa_company.qendpoint.core.rdf.parsers;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TurtleChunkerEofTest {

	@Test
	public void returnsFinalBlockAtEofAfterMultiRead() {
		int payloadBytes = (4 * 1024 * 1024) + 128;
		StringBuilder sb = new StringBuilder(payloadBytes + 64);
		sb.append("<http://ex/s> <http://ex/p> \"");
		for (int i = 0; i < payloadBytes; i++) {
			sb.append('a');
		}
		sb.append("\" .");

		String payload = sb.toString();
		InputStream input = new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8));

		TurtleChunker chunker = new TurtleChunker(input);
		TurtleChunker.BlockIterator iterator = chunker.blockIterator();

		assertTrue("Expected at least one block", iterator.hasNext());
		String block = iterator.next();
		assertEquals("Expected full block at EOF", payload, block);
		assertFalse("Expected single block", iterator.hasNext());
	}
}
