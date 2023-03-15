package com.the_qa_company;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.rdf.parsers.RDFParserSimple;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
import org.apache.commons.io.file.PathUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Benchmark class to test the simple parser
 *
 * @author Antoine Willerval
 */
@State(Scope.Benchmark)
public class SimpleParserBenchmark {
	@Param({"10", "20", "200"})
	int maxLiteralSize;
	@Param({"20", "50", "200"})
	int maxElementSplit;
	Path root;
	Path dataset;

	List<TripleString> datasetGenerated;

	@Setup
	public void setup() throws IOException {
		root = Files.createTempDirectory("qepbench");

		try {
			dataset = root.resolve("dataset.nt");
			dataset.toFile().deleteOnExit();

			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxTriples(100_000, 56)
					.withMaxLiteralSize(maxLiteralSize)
					.withMaxElementSplit(maxElementSplit);

			try (BufferedWriter w = Files.newBufferedWriter(dataset)) {
				Iterator<TripleString> it = supplier.createTripleStringStream();
				datasetGenerated = new ArrayList<>();

				while (it.hasNext()) {
					TripleString triple = it.next();
					datasetGenerated.add(triple.tripleToString());
					triple.dumpNtriple(w);

					if (datasetGenerated.size() % 100 == 0) {
						w.flush();
					}
				}
			}
		} catch (Throwable t) {
			try {
				PathUtils.deleteDirectory(root);
			} catch (Throwable t2) {
				t.addSuppressed(t2);
			}
			throw t;
		}
	}

	@TearDown
	public void complete() throws IOException {
		PathUtils.deleteDirectory(root);
	}

	@Benchmark
	public void simpleParser() throws ParserException {
		RDFParserSimple parser = new RDFParserSimple();

		parser.doParse(dataset,
				"http://example.org/#",
				RDFNotation.NTRIPLES,
				true,
				new TripleChecker()
		);
	}

	class TripleChecker implements RDFParserCallback.RDFCallback {

		int position;

		@Override
		public void processTriple(TripleString triple, long pos) {
			// consume the triple by comparing it with the pre-generated triples
			TripleString excepted = datasetGenerated.get(position++);

			if (!Objects.equals(excepted, triple)) {
				throw new AssertionError(excepted + " != " + triple);
			}
		}
	}
}
