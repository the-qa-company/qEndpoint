package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.iterator.utils.SizedSupplier;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class NTriplesChunkedSourceTest {

	@Test
	public void chunksPullAndParseNTriples() throws Exception {
		String line1 = "<http://ex/s> <http://ex/p> \"o\" .";
		String line2 = "<http://ex/s2> <http://ex/p2> <http://ex/o2> .";
		String nt = line1 + "\n" + line2 + "\n";

		try (InputStream in = new ByteArrayInputStream(nt.getBytes(StandardCharsets.UTF_8));
				NTriplesChunkedSource source = new NTriplesChunkedSource(in, RDFNotation.NTRIPLES, 1L)) {
			SizedSupplier<TripleString> chunk1 = source.get();
			assertNotNull(chunk1);
			assertTrue(chunk1 instanceof java.util.function.Supplier);

			TripleString t1 = chunk1.get();
			assertNotNull(t1);
			assertTrue(t1.getSubject() instanceof ByteString);
			assertTrue(t1.getPredicate() instanceof ByteString);
			assertTrue(t1.getObject() instanceof ByteString);
			assertEquals("http://ex/s", t1.getSubject().toString());
			assertEquals("http://ex/p", t1.getPredicate().toString());
			assertEquals("\"o\"", t1.getObject().toString());
			assertNull(chunk1.get());
			assertEquals((long) line1.length() + 1, chunk1.getSize());

			SizedSupplier<TripleString> chunk2 = source.get();
			assertNotNull(chunk2);
			TripleString t2 = chunk2.get();
			assertNotNull(t2);
			assertTrue(t2.getSubject() instanceof ByteString);
			assertTrue(t2.getPredicate() instanceof ByteString);
			assertTrue(t2.getObject() instanceof ByteString);
			assertEquals("http://ex/s2", t2.getSubject().toString());
			assertEquals("http://ex/p2", t2.getPredicate().toString());
			assertEquals("http://ex/o2", t2.getObject().toString());
			assertNull(chunk2.get());
			assertEquals((long) line2.length() + 1, chunk2.getSize());

			assertNull(source.get());
		}
	}

	@Test
	public void chunksPullAndParseNQuads() throws Exception {
		String line1 = "<http://ex/s> <http://ex/p> \"o\" <http://ex/g> .";
		String line2 = "<http://ex/s2> <http://ex/p2> <http://ex/o2> <http://ex/g2> .";
		String nq = line1 + "\n" + line2 + "\n";

		try (InputStream in = new ByteArrayInputStream(nq.getBytes(StandardCharsets.UTF_8));
				NTriplesChunkedSource source = new NTriplesChunkedSource(in, RDFNotation.NQUAD, 1L)) {
			SizedSupplier<TripleString> chunk1 = source.get();
			assertNotNull(chunk1);
			TripleString t1 = chunk1.get();
			assertNotNull(t1);
			assertTrue(t1.getSubject() instanceof ByteString);
			assertTrue(t1.getPredicate() instanceof ByteString);
			assertTrue(t1.getObject() instanceof ByteString);
			assertTrue(t1.getGraph() instanceof ByteString);
			assertEquals("http://ex/s", t1.getSubject().toString());
			assertEquals("http://ex/p", t1.getPredicate().toString());
			assertEquals("\"o\"", t1.getObject().toString());
			assertEquals("http://ex/g", t1.getGraph().toString());
			assertNull(chunk1.get());

			SizedSupplier<TripleString> chunk2 = source.get();
			assertNotNull(chunk2);
			TripleString t2 = chunk2.get();
			assertNotNull(t2);
			assertTrue(t2.getSubject() instanceof ByteString);
			assertTrue(t2.getPredicate() instanceof ByteString);
			assertTrue(t2.getObject() instanceof ByteString);
			assertTrue(t2.getGraph() instanceof ByteString);
			assertEquals("http://ex/s2", t2.getSubject().toString());
			assertEquals("http://ex/p2", t2.getPredicate().toString());
			assertEquals("http://ex/o2", t2.getObject().toString());
			assertEquals("http://ex/g2", t2.getGraph().toString());
			assertNull(chunk2.get());

			assertNull(source.get());
		}
	}

	@Test
	public void chunksPullAndParseNQuadsBlankNodeGraph() throws Exception {
		String line1 = "<http://ex/s> <http://ex/p> \"o\" _:g .";
		String line2 = "<http://ex/s2> <http://ex/p2> <http://ex/o2> _:g2 .";
		String nq = line1 + "\n" + line2 + "\n";

		try (InputStream in = new ByteArrayInputStream(nq.getBytes(StandardCharsets.UTF_8));
				NTriplesChunkedSource source = new NTriplesChunkedSource(in, RDFNotation.NQUAD, 1L)) {
			SizedSupplier<TripleString> chunk1 = source.get();
			assertNotNull(chunk1);
			TripleString t1 = chunk1.get();
			assertNotNull(t1);
			assertEquals("http://ex/s", t1.getSubject().toString());
			assertEquals("http://ex/p", t1.getPredicate().toString());
			assertEquals("\"o\"", t1.getObject().toString());
			assertNotNull(t1.getGraph());
			assertEquals("_:g", t1.getGraph().toString());
			assertNull(chunk1.get());

			SizedSupplier<TripleString> chunk2 = source.get();
			assertNotNull(chunk2);
			TripleString t2 = chunk2.get();
			assertNotNull(t2);
			assertEquals("http://ex/s2", t2.getSubject().toString());
			assertEquals("http://ex/p2", t2.getPredicate().toString());
			assertEquals("http://ex/o2", t2.getObject().toString());
			assertNotNull(t2.getGraph());
			assertEquals("_:g2", t2.getGraph().toString());
			assertNull(chunk2.get());

			assertNull(source.get());
		}
	}

	@Test
	public void mmapChunksPullAndParseNTriplesFromPath() throws Exception {
		String line1 = "<http://ex/s> <http://ex/p> \"o\" .";
		String line2 = "<http://ex/s2> <http://ex/p2> <http://ex/o2> .";
		String nt = line1 + "\n" + line2 + "\n";

		Path tempFile = Files.createTempFile("ntriples-chunked", ".nt");
		try {
			Files.writeString(tempFile, nt, StandardCharsets.UTF_8);

			try (NTriplesChunkedSource source = new NTriplesChunkedSource(tempFile, RDFNotation.NTRIPLES, 1L)) {
				SizedSupplier<TripleString> chunk1 = source.get();
				assertNotNull(chunk1);
				assertTrue(chunk1 instanceof java.util.function.Supplier);

				TripleString t1 = chunk1.get();
				assertNotNull(t1);
				assertEquals("http://ex/s", t1.getSubject().toString());
				assertEquals("http://ex/p", t1.getPredicate().toString());
				assertEquals("\"o\"", t1.getObject().toString());
				assertNull(chunk1.get());
				assertEquals((long) line1.length() + 1, chunk1.getSize());

				SizedSupplier<TripleString> chunk2 = source.get();
				assertNotNull(chunk2);
				TripleString t2 = chunk2.get();
				assertNotNull(t2);
				assertEquals("http://ex/s2", t2.getSubject().toString());
				assertEquals("http://ex/p2", t2.getPredicate().toString());
				assertEquals("http://ex/o2", t2.getObject().toString());
				assertNull(chunk2.get());
				assertEquals((long) line2.length() + 1, chunk2.getSize());

				assertNull(source.get());
			}
		} finally {
			Files.deleteIfExists(tempFile);
		}
	}

	@Test
	public void mmapChunksPullAndParseNQuadsBlankNodeGraphFromPath() throws Exception {
		String line1 = "<http://ex/s> <http://ex/p> \"o\" _:g .";
		String line2 = "<http://ex/s2> <http://ex/p2> <http://ex/o2> _:g2 .";
		String nq = line1 + "\n" + line2 + "\n";

		Path tempFile = Files.createTempFile("nquads-chunked", ".nq");
		try {
			Files.writeString(tempFile, nq, StandardCharsets.UTF_8);

			try (NTriplesChunkedSource source = new NTriplesChunkedSource(tempFile, RDFNotation.NQUAD, 1L)) {
				SizedSupplier<TripleString> chunk1 = source.get();
				assertNotNull(chunk1);

				TripleString t1 = chunk1.get();
				assertNotNull(t1);
				assertEquals("http://ex/s", t1.getSubject().toString());
				assertEquals("http://ex/p", t1.getPredicate().toString());
				assertEquals("\"o\"", t1.getObject().toString());
				assertNotNull(t1.getGraph());
				assertEquals("_:g", t1.getGraph().toString());
				assertNull(chunk1.get());

				SizedSupplier<TripleString> chunk2 = source.get();
				assertNotNull(chunk2);
				TripleString t2 = chunk2.get();
				assertNotNull(t2);
				assertEquals("http://ex/s2", t2.getSubject().toString());
				assertEquals("http://ex/p2", t2.getPredicate().toString());
				assertEquals("http://ex/o2", t2.getObject().toString());
				assertNotNull(t2.getGraph());
				assertEquals("_:g2", t2.getGraph().toString());
				assertNull(chunk2.get());

				assertNull(source.get());
			}
		} finally {
			Files.deleteIfExists(tempFile);
		}
	}
}
