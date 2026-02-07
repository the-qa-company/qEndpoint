package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NTriplesChunkedSourceTest {

	@Test
	public void chunksPullAndParseNTriples() throws Exception {
		String line1 = "<http://ex/s> <http://ex/p> \"o\" .";
		String line2 = "<http://ex/s2> <http://ex/p2> <http://ex/o2> .";
		String nt = line1 + "\n" + line2 + "\n";

		InputStream in = new ByteArrayInputStream(nt.getBytes(StandardCharsets.UTF_8));

		Class<?> cls;
		try {
			cls = Class.forName("com.the_qa_company.qendpoint.core.rdf.parsers.NTriplesChunkedSource");
		} catch (ClassNotFoundException e) {
			fail("Missing class com.the_qa_company.qendpoint.core.rdf.parsers.NTriplesChunkedSource");
			return;
		}

		Constructor<?> ctor = cls.getConstructor(InputStream.class, RDFNotation.class, long.class);

		Object source = ctor.newInstance(in, RDFNotation.NTRIPLES, 1L);
		Method getChunk = cls.getMethod("get");

		// chunk #1 (budget too small to read more than the first line)
		Object chunk1 = getChunk.invoke(source);
		assertNotNull(chunk1);
		assertTrue(chunk1 instanceof Supplier);

		Supplier<?> flux1 = (Supplier<?>) chunk1;
		TripleString t1 = (TripleString) flux1.get();
		assertNotNull(t1);
		assertTrue(t1.getSubject() instanceof ByteString);
		assertTrue(t1.getPredicate() instanceof ByteString);
		assertTrue(t1.getObject() instanceof ByteString);
		assertEquals("http://ex/s", t1.getSubject().toString());
		assertEquals("http://ex/p", t1.getPredicate().toString());
		assertEquals("\"o\"", t1.getObject().toString());
		assertNull(flux1.get());

		long size1 = (long) chunk1.getClass().getMethod("getSize").invoke(chunk1);
		assertEquals((long) line1.length() + 1, size1);

		// chunk #2
		Object chunk2 = getChunk.invoke(source);
		assertNotNull(chunk2);
		Supplier<?> flux2 = (Supplier<?>) chunk2;

		TripleString t2 = (TripleString) flux2.get();
		assertNotNull(t2);
		assertTrue(t2.getSubject() instanceof ByteString);
		assertTrue(t2.getPredicate() instanceof ByteString);
		assertTrue(t2.getObject() instanceof ByteString);
		assertEquals("http://ex/s2", t2.getSubject().toString());
		assertEquals("http://ex/p2", t2.getPredicate().toString());
		assertEquals("http://ex/o2", t2.getObject().toString());
		assertNull(flux2.get());

		long size2 = (long) chunk2.getClass().getMethod("getSize").invoke(chunk2);
		assertEquals((long) line2.length() + 1, size2);

		// EOF
		assertNull(getChunk.invoke(source));
		((AutoCloseable) source).close();
	}

	@Test
	public void chunksPullAndParseNQuads() throws Exception {
		String line1 = "<http://ex/s> <http://ex/p> \"o\" <http://ex/g> .";
		String line2 = "<http://ex/s2> <http://ex/p2> <http://ex/o2> <http://ex/g2> .";
		String nq = line1 + "\n" + line2 + "\n";

		InputStream in = new ByteArrayInputStream(nq.getBytes(StandardCharsets.UTF_8));

		Class<?> cls;
		try {
			cls = Class.forName("com.the_qa_company.qendpoint.core.rdf.parsers.NTriplesChunkedSource");
		} catch (ClassNotFoundException e) {
			fail("Missing class com.the_qa_company.qendpoint.core.rdf.parsers.NTriplesChunkedSource");
			return;
		}

		Constructor<?> ctor = cls.getConstructor(InputStream.class, RDFNotation.class, long.class);

		Object source = ctor.newInstance(in, RDFNotation.NQUAD, 1L);
		Method getChunk = cls.getMethod("get");

		Object chunk1 = getChunk.invoke(source);
		assertNotNull(chunk1);
		Supplier<?> flux1 = (Supplier<?>) chunk1;
		TripleString t1 = (TripleString) flux1.get();
		assertNotNull(t1);
		assertTrue(t1.getSubject() instanceof ByteString);
		assertTrue(t1.getPredicate() instanceof ByteString);
		assertTrue(t1.getObject() instanceof ByteString);
		assertTrue(t1.getGraph() instanceof ByteString);
		assertEquals("http://ex/s", t1.getSubject().toString());
		assertEquals("http://ex/p", t1.getPredicate().toString());
		assertEquals("\"o\"", t1.getObject().toString());
		assertEquals("http://ex/g", t1.getGraph().toString());
		assertNull(flux1.get());

		Object chunk2 = getChunk.invoke(source);
		assertNotNull(chunk2);
		Supplier<?> flux2 = (Supplier<?>) chunk2;
		TripleString t2 = (TripleString) flux2.get();
		assertNotNull(t2);
		assertTrue(t2.getSubject() instanceof ByteString);
		assertTrue(t2.getPredicate() instanceof ByteString);
		assertTrue(t2.getObject() instanceof ByteString);
		assertTrue(t2.getGraph() instanceof ByteString);
		assertEquals("http://ex/s2", t2.getSubject().toString());
		assertEquals("http://ex/p2", t2.getPredicate().toString());
		assertEquals("http://ex/o2", t2.getObject().toString());
		assertEquals("http://ex/g2", t2.getGraph().toString());
		assertNull(flux2.get());

		assertNull(getChunk.invoke(source));
		((AutoCloseable) source).close();
	}

	@Test
	public void chunksPullAndParseNQuadsBlankNodeGraph() throws Exception {
		String line1 = "<http://ex/s> <http://ex/p> \"o\" _:g .";
		String line2 = "<http://ex/s2> <http://ex/p2> <http://ex/o2> _:g2 .";
		String nq = line1 + "\n" + line2 + "\n";

		InputStream in = new ByteArrayInputStream(nq.getBytes(StandardCharsets.UTF_8));

		Class<?> cls;
		try {
			cls = Class.forName("com.the_qa_company.qendpoint.core.rdf.parsers.NTriplesChunkedSource");
		} catch (ClassNotFoundException e) {
			fail("Missing class com.the_qa_company.qendpoint.core.rdf.parsers.NTriplesChunkedSource");
			return;
		}

		Constructor<?> ctor = cls.getConstructor(InputStream.class, RDFNotation.class, long.class);

		Object source = ctor.newInstance(in, RDFNotation.NQUAD, 1L);
		Method getChunk = cls.getMethod("get");

		Object chunk1 = getChunk.invoke(source);
		assertNotNull(chunk1);
		Supplier<?> flux1 = (Supplier<?>) chunk1;
		TripleString t1 = (TripleString) flux1.get();
		assertNotNull(t1);
		assertEquals("http://ex/s", t1.getSubject().toString());
		assertEquals("http://ex/p", t1.getPredicate().toString());
		assertEquals("\"o\"", t1.getObject().toString());
		assertNotNull(t1.getGraph());
		assertEquals("_:g", t1.getGraph().toString());
		assertNull(flux1.get());

		Object chunk2 = getChunk.invoke(source);
		assertNotNull(chunk2);
		Supplier<?> flux2 = (Supplier<?>) chunk2;
		TripleString t2 = (TripleString) flux2.get();
		assertNotNull(t2);
		assertEquals("http://ex/s2", t2.getSubject().toString());
		assertEquals("http://ex/p2", t2.getPredicate().toString());
		assertEquals("http://ex/o2", t2.getObject().toString());
		assertNotNull(t2.getGraph());
		assertEquals("_:g2", t2.getGraph().toString());
		assertNull(flux2.get());

		assertNull(getChunk.invoke(source));
		((AutoCloseable) source).close();
	}

	@Test
	public void mmapChunksPullAndParseNTriplesFromPath() throws Exception {
		String line1 = "<http://ex/s> <http://ex/p> \"o\" .";
		String line2 = "<http://ex/s2> <http://ex/p2> <http://ex/o2> .";
		String nt = line1 + "\n" + line2 + "\n";

		Path tempFile = Files.createTempFile("ntriples-chunked", ".nt");
		try {
			Files.writeString(tempFile, nt, StandardCharsets.UTF_8);

			Class<?> cls;
			try {
				cls = Class.forName("com.the_qa_company.qendpoint.core.rdf.parsers.NTriplesChunkedSource");
			} catch (ClassNotFoundException e) {
				fail("Missing class com.the_qa_company.qendpoint.core.rdf.parsers.NTriplesChunkedSource");
				return;
			}

			Constructor<?> ctor;
			try {
				ctor = cls.getConstructor(Path.class, RDFNotation.class, long.class);
			} catch (NoSuchMethodException e) {
				fail("Missing Path constructor on NTriplesChunkedSource");
				return;
			}

			Object source = ctor.newInstance(tempFile, RDFNotation.NTRIPLES, 1L);
			Method getChunk = cls.getMethod("get");

			Object chunk1 = getChunk.invoke(source);
			assertNotNull(chunk1);
			assertTrue(chunk1 instanceof Supplier);

			Supplier<?> flux1 = (Supplier<?>) chunk1;
			TripleString t1 = (TripleString) flux1.get();
			assertNotNull(t1);
			assertEquals("http://ex/s", t1.getSubject().toString());
			assertEquals("http://ex/p", t1.getPredicate().toString());
			assertEquals("\"o\"", t1.getObject().toString());
			assertNull(flux1.get());

			long size1 = (long) chunk1.getClass().getMethod("getSize").invoke(chunk1);
			assertEquals((long) line1.length() + 1, size1);

			Object chunk2 = getChunk.invoke(source);
			assertNotNull(chunk2);
			Supplier<?> flux2 = (Supplier<?>) chunk2;

			TripleString t2 = (TripleString) flux2.get();
			assertNotNull(t2);
			assertEquals("http://ex/s2", t2.getSubject().toString());
			assertEquals("http://ex/p2", t2.getPredicate().toString());
			assertEquals("http://ex/o2", t2.getObject().toString());
			assertNull(flux2.get());

			long size2 = (long) chunk2.getClass().getMethod("getSize").invoke(chunk2);
			assertEquals((long) line2.length() + 1, size2);

			assertNull(getChunk.invoke(source));
			((AutoCloseable) source).close();
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

			Class<?> cls;
			try {
				cls = Class.forName("com.the_qa_company.qendpoint.core.rdf.parsers.NTriplesChunkedSource");
			} catch (ClassNotFoundException e) {
				fail("Missing class com.the_qa_company.qendpoint.core.rdf.parsers.NTriplesChunkedSource");
				return;
			}

			Constructor<?> ctor;
			try {
				ctor = cls.getConstructor(Path.class, RDFNotation.class, long.class);
			} catch (NoSuchMethodException e) {
				fail("Missing Path constructor on NTriplesChunkedSource");
				return;
			}

			Object source = ctor.newInstance(tempFile, RDFNotation.NQUAD, 1L);
			Method getChunk = cls.getMethod("get");

			Object chunk1 = getChunk.invoke(source);
			assertNotNull(chunk1);
			Supplier<?> flux1 = (Supplier<?>) chunk1;

			TripleString t1 = (TripleString) flux1.get();
			assertNotNull(t1);
			assertEquals("http://ex/s", t1.getSubject().toString());
			assertEquals("http://ex/p", t1.getPredicate().toString());
			assertEquals("\"o\"", t1.getObject().toString());
			assertNotNull(t1.getGraph());
			assertEquals("_:g", t1.getGraph().toString());
			assertNull(flux1.get());

			Object chunk2 = getChunk.invoke(source);
			assertNotNull(chunk2);
			Supplier<?> flux2 = (Supplier<?>) chunk2;

			TripleString t2 = (TripleString) flux2.get();
			assertNotNull(t2);
			assertEquals("http://ex/s2", t2.getSubject().toString());
			assertEquals("http://ex/p2", t2.getPredicate().toString());
			assertEquals("http://ex/o2", t2.getObject().toString());
			assertNotNull(t2.getGraph());
			assertEquals("_:g2", t2.getGraph().toString());
			assertNull(flux2.get());

			assertNull(getChunk.invoke(source));
			((AutoCloseable) source).close();
		} finally {
			Files.deleteIfExists(tempFile);
		}
	}
}
