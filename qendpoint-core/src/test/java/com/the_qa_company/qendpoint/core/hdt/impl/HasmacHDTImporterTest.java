package com.the_qa_company.qendpoint.core.hdt.impl;

import com.the_qa_company.qendpoint.core.enums.CompressionType;
import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.iterator.utils.PipedCopyIterator;
import com.the_qa_company.qendpoint.core.options.HDTSpecification;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.rdf.RDFParserFactory;
import com.the_qa_company.qendpoint.core.rdf.parsers.ConcurrentInputStream;
import com.the_qa_company.qendpoint.core.rdf.parsers.RDFParserRIOT;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.triples.impl.utils.HDTTestUtils;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.lang.LabelToNode;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

public class HasmacHDTImporterTest {

	private final HDTSpecification spec;

	public HasmacHDTImporterTest() {
		spec = new HDTSpecification();
		spec.set("loader.type", "one-pass");
		spec.set("loader.bnode.seed", "1234567");
	}

	private Iterator<TripleString> asIt(String file) throws ParserException {
		List<TripleString> triples = new ArrayList<>();
		RDFNotation notation = RDFNotation.guess(file);
		RDFParserCallback parser = RDFParserFactory.getParserCallback(notation);
		parser.doParse(file, HDTTestUtils.BASE_URI, notation, true, (triple, pos) -> {
			// force duplication of the triple string data
			triples.add(new TripleString(triple.getSubject().toString(), triple.getPredicate().toString(),
					triple.getObject().toString()));
		});
		return triples.iterator();
	}

	@Test
	public void testGz() throws ParserException, IOException {
		FileInputStream fileStream = new FileInputStream(
				"/Users/havardottestad/Documents/Programming/qEndpoint2/indexing/latest-truthy.nt.gz");

		try (InputStream uncompressed = IOUtil.asUncompressed(fileStream, CompressionType.GZIP);
				BufferedReader reader = new BufferedReader(new InputStreamReader(uncompressed))) {
			long sum = 0;
			String line;
			long startTime = System.currentTimeMillis();
			int lineCount = 0;
			int checkpoint = 1000000;

			while ((line = reader.readLine()) != null) {
				sum += line.length();
				lineCount++;
				if (lineCount == checkpoint) {
					long currentTime = System.currentTimeMillis();
					long elapsedTime = currentTime - startTime; // in
																// milliseconds
					int linesPerSecond = ((int) Math.floor(checkpoint / (elapsedTime / 1000.0)));

					// TODO: print linesPerSecond with thousands separator
					System.out.println(String.format("Lines per second: %,d", linesPerSecond));

					startTime = currentTime; // reset start time for the next
												// checkpoint
					lineCount = 0; // reset line count for the next checkpoint
				}
			}
			System.out.println(sum);
		}
	}

	@Test
	public void concurentInputStreamTest() throws ParserException, IOException {
		try (InputStream fileStream = new FileInputStream(
				"/Users/havardottestad/Documents/Programming/qEndpoint2/indexing/latest-truthy.nt.gz")) {

			InputStream uncompressed = IOUtil.asUncompressed(fileStream, CompressionType.GZIP);

			LongAdder longAdder = new LongAdder();

			ConcurrentInputStream cs = new ConcurrentInputStream(uncompressed, 10);

			InputStream bnodes = cs.getBnodeStream();

			var threads = new ArrayList<Thread>();

			Thread e1 = new Thread(() -> {
				BufferedReader reader = new BufferedReader(new InputStreamReader(bnodes));
				while (true) {
					try {
						if (!(reader.readLine() != null))
							break;
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
					longAdder.increment();
				}

			});
			e1.setName("BNode parser");
			threads.add(e1);

			InputStream[] streams = cs.getStreams();
			int i = 0;
			for (InputStream s : streams) {
				int temp = i + 1;
				Thread e = new Thread(() -> {
					BufferedReader reader = new BufferedReader(new InputStreamReader(s));
					while (true) {
						try {
							if (!(reader.readLine() != null))
								break;
						} catch (IOException e2) {
							throw new RuntimeException(e2);
						}
						longAdder.increment();
					}
				});
				i++;
				e.setName("Stream parser " + i);
				threads.add(e);

			}

			threads.forEach(Thread::start);

			new Thread(() -> {
				while (true) {
					try {
						Thread.sleep(10 * 1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					System.out.println(String.format("Lines per second: %,d", longAdder.sumThenReset() / 10));

				}
			}).start();

			for (Thread thread : threads) {
				try {
					thread.join();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}

	}

	@Test
	public void concurrentParsing() throws ParserException, IOException {
		try (InputStream fileStream = new FileInputStream(
				"/Users/havardottestad/Documents/Programming/qEndpoint2/indexing/latest-truthy.nt.gz")) {

			InputStream uncompressed = IOUtil.asUncompressed(fileStream, CompressionType.GZIP);

			LongAdder longAdder = new LongAdder();

			ConcurrentInputStream cs = new ConcurrentInputStream(uncompressed, 11);

			InputStream bnodes = cs.getBnodeStream();

			var threads = new ArrayList<Thread>();

			RDFParser parser1 = RDFParser.source(bnodes).base("").lang(Lang.NTRIPLES)
					.labelToNode(LabelToNode.createUseLabelAsGiven()).build();
			Thread e1 = new Thread(() -> {
				parser1.parse(new RDFParserRIOT.ElemStringBuffer((triple, pos) -> longAdder.increment()));
			});
			e1.setName("BNode parser");
			threads.add(e1);

			InputStream[] streams = cs.getStreams();
			int i = 0;
			for (InputStream s : streams) {
				int temp = i + 1;
				RDFParser parser = RDFParser.source(s).base("").lang(Lang.NTRIPLES)
						.labelToNode(LabelToNode.createUseLabelAsGiven()).build();
				Thread e = new Thread(() -> {
					parser.parse(new RDFParserRIOT.ElemStringBuffer((triple, pos) -> longAdder.increment()));
				});
				i++;
				e.setName("Stream parser " + i);
				threads.add(e);

			}

			threads.forEach(Thread::start);

			new Thread(() -> {
				while (true) {
					try {
						Thread.sleep(10 * 1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					System.out.println(String.format("Lines per second: %,d", longAdder.sumThenReset() / 10));
				}
			}).start();

			for (Thread thread : threads) {
				try {
					thread.join();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}

	}

}
