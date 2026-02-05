package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class RDFParserRIOTParallelFailureTest {
	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void parallelParsePropagatesWorkerFailures() throws Exception {
		String payload = "<http://ex/s1> <http://ex/p> \"o1\" .\n" + "<http://ex/s2> <http://ex/p> \"unterminated .\n"
				+ "_:b1 <http://ex/p> \"o3\" .\n";

		InputStream input = new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8));

		try {
			new RDFParserRIOT().doParse(input, "http://base/", RDFNotation.NTRIPLES, true, (triple, pos) -> {}, true);
			fail("Expected parser to surface worker failure");
		} catch (ParserException expected) {
			// expected
		}
	}

	@Test
	public void fileParseEntryPointDoesNotForceParallelWorkers() throws Exception {
		String payload = "<http://ex/s1> <http://ex/p> \"o1\" .\n" + "_:b1 <http://ex/p> \"o2\" .\n"
				+ "<http://ex/s3> <http://ex/p> \"o3\" .\n";
		Path file = tempDir.newFile("riot-file-entrypoint.nt").toPath();
		Files.writeString(file, payload, StandardCharsets.UTF_8);

		Set<String> callbackThreads = ConcurrentHashMap.newKeySet();
		new RDFParserRIOT().doParse(file.toString(), "http://base/", RDFNotation.NTRIPLES, true,
				(triple, pos) -> callbackThreads.add(Thread.currentThread().getName()));

		boolean usedWorkerThread = callbackThreads.stream()
				.anyMatch(name -> name.startsWith("Stream parser") || name.startsWith("BNode parser"));
		assertFalse("file parse entrypoint forced parallel worker callbacks", usedWorkerThread);
	}

	@Test
	public void parallelNTriplesParseSurfacesSourceReadFailures() throws Exception {
		String payload = "<http://ex/s1> <http://ex/p> \"o1\" .\n" + "_:b1 <http://ex/p> \"o2\" .\n";
		InputStream input = new FailingAfterDataInputStream(payload);

		try {
			new RDFParserRIOT().doParse(input, "http://base/", RDFNotation.NTRIPLES, true, (triple, pos) -> {}, true);
			fail("Expected parser to surface source read failure");
		} catch (ParserException expected) {
			// expected
		}
	}

	@Test
	public void parallelTurtleParseSurfacesSourceReadFailures() throws Exception {
		String payload = "@prefix ex: <http://ex/> .\n" + "ex:s1 ex:p \"o1\" .\n" + "_:b1 ex:p \"o2\" .\n";
		InputStream input = new FailingAfterDataInputStream(payload);

		try {
			new RDFParserRIOT().doParse(input, "http://base/", RDFNotation.TURTLE, true, (triple, pos) -> {}, true);
			fail("Expected parser to surface source read failure");
		} catch (ParserException expected) {
			// expected
		}
	}

	private static final class FailingAfterDataInputStream extends InputStream {
		private static final IOException FAILURE = new IOException("Injected source read failure");
		private final byte[] data;
		private int index;

		private FailingAfterDataInputStream(String payload) {
			this.data = payload.getBytes(StandardCharsets.UTF_8);
		}

		@Override
		public int read() throws IOException {
			if (index < data.length) {
				return data[index++] & 0xFF;
			}
			throw FAILURE;
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			if (len == 0) {
				return 0;
			}
			if (index < data.length) {
				int available = data.length - index;
				int read = Math.min(len, available);
				System.arraycopy(data, index, b, off, read);
				index += read;
				return read;
			}
			throw FAILURE;
		}
	}
}
