package com.the_qa_company.qendpoint.core.util;

import com.the_qa_company.qendpoint.core.enums.CompressionType;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.iterator.utils.MapIterator;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.quads.QuadString;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.concurrent.ExceptionThread;
import com.the_qa_company.qendpoint.core.util.string.ByteStringUtil;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Random;
import java.util.zip.GZIPOutputStream;

/**
 * Utility class to create fake large dataset
 *
 * @author Antoine Willerval
 */
public class LargeFakeDataSetStreamSupplier {

	private static final Charset DEFAULT_CHARSET = ByteStringUtil.STRING_ENCODING;

	/**
	 * create a lowercase name from a number, to create string without any
	 * number in it
	 *
	 * @param i id
	 * @return string
	 */
	public static String stringNameOfInt(int i, boolean unicode) {
		StringBuilder out = new StringBuilder();
		if (unicode) {
			return new String(Character.toChars(Math.min(i, Character.MAX_CODE_POINT)));
		} else {
			String table = "abcdefghijklmnopqrstuvwxyz";
			int c = i;
			do {
				out.append(table.charAt(c % table.length()));
				c /= table.length();
			} while (c != 0);
		}
		return out.toString();
	}

	/**
	 * create a lowercase name from a number, to create string without any
	 * number in it
	 *
	 * @param i id
	 * @return string
	 */
	public static String stringNameOfInt(int i) {
		return stringNameOfInt(i, false);
	}

	/**
	 * estimate the size of a triple
	 *
	 * @param triple the triple
	 * @return the size in byte to store the triple
	 */
	public static long estimateTripleSize(TripleString triple) {
		try {
			return triple.asNtriple().toString().getBytes(DEFAULT_CHARSET).length;
		} catch (IOException e) {
			throw new RuntimeException("Can't estimate the size of the triple " + triple, e);
		}
	}

	/**
	 * create a supplier with a max size
	 *
	 * @param maxSize the max size
	 * @param seed    the seed of the supplier, the same seed will create the
	 *                same supplier
	 * @return supplier
	 */
	public static LargeFakeDataSetStreamSupplier createSupplierWithMaxSize(long maxSize, long seed) {
		return new LargeFakeDataSetStreamSupplier(maxSize, Long.MAX_VALUE, seed);
	}

	/**
	 * create a supplier with a max count
	 *
	 * @param maxTriples the max number of triples
	 * @param seed       the seed of the supplier, the same seed will create the
	 *                   same supplier
	 * @return supplier
	 */
	public static LargeFakeDataSetStreamSupplier createSupplierWithMaxTriples(long maxTriples, long seed) {
		return new LargeFakeDataSetStreamSupplier(Long.MAX_VALUE, maxTriples, seed);
	}

	/**
	 * create a supplier without a max count
	 *
	 * @param seed the seed of the supplier, the same seed will create the same
	 *             supplier
	 * @return supplier
	 */
	public static LargeFakeDataSetStreamSupplier createInfinite(long seed) {
		return new LargeFakeDataSetStreamSupplier(Long.MAX_VALUE, Long.MAX_VALUE, seed);
	}

	private final long seed;
	private Random random;
	private long maxSize;
	private long maxTriples;
	public int maxFakeType = 10;
	public int maxLiteralSize = 2;
	public int maxGraph = 10;
	public int maxElementSplit = Integer.MAX_VALUE;
	private long slowStream;
	private boolean unicode;
	private TripleString buffer;
	private TripleString next;
	private boolean nquad;

	private LargeFakeDataSetStreamSupplier(long maxSize, long maxTriples, long seed) {
		this.maxSize = maxSize;
		this.maxTriples = maxTriples;
		this.seed = seed;
		reset();
	}

	/**
	 * reset the supplier like it was just created
	 */
	public void reset() {
		random = new Random(seed);
		next = null;
		buffer = null;
	}

	/**
	 * @return iterator of triples
	 */
	public Iterator<TripleString> createTripleStringStream() {
		return new FakeStatementIterator();
	}

	/**
	 * create a nt file from the stream
	 *
	 * @param file the file to write
	 * @throws IOException io exception
	 * @see #createNTFile(java.nio.file.Path)
	 */
	public void createNTFile(String file) throws IOException {
		createNTFile(Path.of(file));
	}

	/**
	 * create a nt file from the stream
	 *
	 * @param file the file to write
	 * @throws IOException io exception
	 * @see #createNTFile(java.lang.String)
	 */
	public void createNTFile(Path file) throws IOException {
		try (BufferedWriter writer = Files.newBufferedWriter(file)) {
			createNTFile(writer);
		}
	}

	/**
	 * create a nt file from the stream
	 *
	 * @param writer the writer to write
	 * @throws IOException io exception
	 * @see #createNTFile(java.lang.String)
	 */
	public void createNTFile(Writer writer) throws IOException {
		for (Iterator<TripleString> it = createTripleStringStream(); it.hasNext();) {
			it.next().dumpNtriple(writer);
		}
	}

	/**
	 * create a threaded stream (to close!) with a particular compression
	 *
	 * @param compressionType compression type
	 * @return threaded stream
	 * @throws IOException io exception
	 */
	public ThreadedStream createNTInputStream(CompressionType compressionType) throws IOException {
		PipedOutputStream pout = new PipedOutputStream();
		InputStream is = new PipedInputStream(pout);
		OutputStream out;

		if (compressionType != null) {
			out = switch (compressionType) {
			case NONE -> pout;
			case XZ -> new XZCompressorOutputStream(pout);
			case BZIP -> new BZip2CompressorOutputStream(pout);
			case GZIP -> new GZIPOutputStream(pout);
			};
		} else {
			out = pout;
		}

		Iterator<TripleString> it = createTripleStringStream();

		ExceptionThread run = new ExceptionThread(() -> {
			try (PrintStream ps = new PrintStream(out, true)) {
				while (it.hasNext()) {
					it.next().dumpNtriple(ps);
				}
			}
		}, "ThreadedFakedStream");
		run.start();

		return new ThreadedStream(run, is);
	}

	/**
	 * create an HDT from the stream
	 *
	 * @param spec hdt options
	 * @return hdt
	 * @throws ParserException parsing exception
	 * @throws IOException     io exception
	 */
	public HDT createFakeHDT(HDTOptions spec) throws ParserException, IOException {
		return HDTManager.generateHDT(createTripleStringStream(), "http://w", spec, null);
	}

	/**
	 * create an HDT from the stream and save it to a file
	 *
	 * @param spec     hdt options
	 * @param location save location
	 * @throws ParserException parsing exception
	 * @throws IOException     io exception
	 */
	public void createAndSaveFakeHDT(HDTOptions spec, Path location) throws ParserException, IOException {
		createAndSaveFakeHDT(spec, location.toAbsolutePath().toString());
	}

	/**
	 * create an HDT from the stream and save it to a file
	 *
	 * @param spec     hdt options
	 * @param location save location
	 * @throws ParserException parsing exception
	 * @throws IOException     io exception
	 */
	public void createAndSaveFakeHDT(HDTOptions spec, String location) throws ParserException, IOException {
		try (HDT hdt = createFakeHDT(spec)) {
			hdt.saveToHDT(location, null);
		}
	}

	private CharSequence createGraph() {
		if (maxGraph == 0) {
			return "";
		}
		int rnd = random.nextInt(10);
		if (rnd < 4) {
			return ""; // no graph
		}
		if (rnd == 4) {
			return "_:bnode" + random.nextInt(maxGraph / 2);
		}
		return "http://test.org/#graph" + random.nextInt(maxGraph / 2);
	}

	private CharSequence createResource() {
		if (random.nextInt(10) == 0) {
			return "_:bnode" + random.nextInt(maxElementSplit / 10);
		}
		return createIRI();
	}

	private CharSequence createIRI() {
		return "http://w" + random.nextInt(maxElementSplit) + "i.test.org/#Obj" + random.nextInt(maxElementSplit);
	}

	private CharSequence createType() {
		return "http://wti.test.org/#Obj" + random.nextInt(maxFakeType);
	}

	private CharSequence createValue() {
		if (random.nextBoolean()) {
			return createResource();
		}
		int size = random.nextInt(maxLiteralSize);
		StringBuilder litText = new StringBuilder();
		for (int i = 0; i < size; i++) {
			litText.append(stringNameOfInt(
					unicode ? random.nextInt(Character.MAX_CODE_POINT - 30) + 30 : random.nextInt(maxElementSplit),
					unicode));
		}
		String text = "\"" + litText + "\"";
		int litType = random.nextInt(3);
		if (litType == 1) {
			// language node
			return text + "@" + stringNameOfInt(random.nextInt(maxElementSplit));
		} else if (litType == 2) {
			// typed node
			return text + "^^<" + createType() + ">";
		} else {
			// no type/language node
			return text;
		}
	}

	/**
	 * @return the stream of the objects
	 */
	public Iterator<? extends CharSequence> objectIterator() {
		return new MapIterator<>(createTripleStringStream(), TripleString::getObject);
	}

	private class FakeStatementIterator implements Iterator<TripleString> {
		private long size;
		private long count = 0;
		private boolean init;

		private final long maxTriples;
		private final long maxSize;

		FakeStatementIterator() {
			this.maxSize = LargeFakeDataSetStreamSupplier.this.maxSize;
			this.maxTriples = LargeFakeDataSetStreamSupplier.this.maxTriples;
		}

		@Override
		public boolean hasNext() {
			if (!init) {
				init = true;
				if (next != null) {
					long estimation = estimateTripleSize(next);
					size += estimation;
					count++;
				}
			}
			if (size >= maxSize || count > maxTriples) {
				return false;
			}
			if (next != null) {
				return true;
			}

			CharSequence resource = createResource();
			CharSequence iri = createIRI();
			CharSequence value = createValue();

			if (buffer != null) {
				buffer.setAll(resource, iri, value);
				if (nquad) {
					buffer.setGraph(createGraph());
				}
				next = buffer;
			} else {
				if (nquad) {
					next = new QuadString(resource, iri, value, createGraph());
				} else {
					next = new TripleString(resource, iri, value);
				}
			}

			if (slowStream > 0) {
				try {
					Thread.sleep(slowStream);
				} catch (InterruptedException e) {
					throw new AssertionError(e);
				}
			}

			long estimation = estimateTripleSize(next);
			size += estimation;
			count++;

			return size < maxSize && count <= maxTriples;
		}

		@Override
		public TripleString next() {
			if (!hasNext()) {
				return null;
			}
			TripleString next = LargeFakeDataSetStreamSupplier.this.next;
			LargeFakeDataSetStreamSupplier.this.next = null;
			return next;
		}
	}

	/**
	 * set the max size
	 *
	 * @param maxSize max size
	 * @return this
	 */
	public LargeFakeDataSetStreamSupplier withMaxSize(long maxSize) {
		this.maxSize = maxSize;
		return this;
	}

	/**
	 * set the max triples count
	 *
	 * @param maxTriples max triples count
	 * @return this
	 */
	public LargeFakeDataSetStreamSupplier withMaxTriples(long maxTriples) {
		this.maxTriples = maxTriples;
		return this;
	}

	/**
	 * set the maximum number of fake type
	 *
	 * @param maxFakeType maximum number
	 * @return this
	 */
	public LargeFakeDataSetStreamSupplier withMaxFakeType(int maxFakeType) {
		this.maxFakeType = maxFakeType;
		return this;
	}

	/**
	 * set the maximum element split number
	 *
	 * @param maxElementSplit maximum number
	 * @return this
	 */
	public LargeFakeDataSetStreamSupplier withMaxElementSplit(int maxElementSplit) {
		this.maxElementSplit = maxElementSplit;
		return this;
	}

	/**
	 * set the maximum literal size
	 *
	 * @param maxLiteralSize maximum number
	 * @return this
	 */
	public LargeFakeDataSetStreamSupplier withMaxLiteralSize(int maxLiteralSize) {
		this.maxLiteralSize = maxLiteralSize;
		return this;

	}

	/**
	 * allow using unicode or not in the literals
	 *
	 * @param unicode unicode
	 * @return this
	 */
	public LargeFakeDataSetStreamSupplier withUnicode(boolean unicode) {
		this.unicode = unicode;
		return this;
	}

	/**
	 * add a latency to the stream generation
	 *
	 * @param slowStream latency (millis)
	 * @return this
	 */
	public LargeFakeDataSetStreamSupplier withSlowStream(long slowStream) {
		this.slowStream = slowStream;
		return this;
	}

	/**
	 * use the same {@link TripleString} object, better to simulate the
	 * RDFParser outputs
	 *
	 * @param sameTripleString use same triple
	 * @return this
	 */
	public LargeFakeDataSetStreamSupplier withSameTripleString(boolean sameTripleString) {
		if (sameTripleString) {
			if (nquad) {
				buffer = new QuadString();
			} else {
				buffer = new TripleString();
			}
		} else {
			buffer = null;
		}
		return this;
	}

	/**
	 * generate quad with the triple strings
	 *
	 * @param quad quads
	 * @return this
	 */
	public LargeFakeDataSetStreamSupplier withQuads(boolean quad) {
		if (this.nquad == quad) {
			return this;
		}
		this.nquad = quad;
		if (buffer != null) {
			// we need to reset the buffer
			TripleString old = buffer;
			if (quad) {
				buffer = new QuadString(old);
			} else {
				buffer = new TripleString(old);
			}
		}
		return this;
	}

	/**
	 * set the maximum number of graph with quad generation
	 *
	 * @param maxGraph max number of graph (excluding the default graph)
	 * @return this
	 */
	public LargeFakeDataSetStreamSupplier withMaxGraph(int maxGraph) {
		this.maxGraph = maxGraph;
		return this;
	}

	/**
	 * Stream connected to a thread to interrupt in case of Exception
	 */
	public static class ThreadedStream {
		private final ExceptionThread thread;
		private final InputStream stream;

		public ThreadedStream(ExceptionThread thread, InputStream stream) {
			this.thread = thread;
			this.stream = stream;
		}

		/**
		 * @return the thread
		 */
		public ExceptionThread getThread() {
			return thread;
		}

		/**
		 * @return the stream
		 */
		public InputStream getStream() {
			return stream;
		}
	}
}
