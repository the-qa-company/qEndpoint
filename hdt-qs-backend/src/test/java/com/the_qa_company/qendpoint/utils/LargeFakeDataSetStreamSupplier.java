package com.the_qa_company.qendpoint.utils;

import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.utils.sail.MultiLanguageTypedLuceneIndexTest;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Iterator;
import java.util.Random;

public class LargeFakeDataSetStreamSupplier {
	private final long seed;
	private Random random;
	private final long maxSize;
	public int maxFakeType = 10;
	private final ValueFactory valueFactory = SimpleValueFactory.getInstance();

	public LargeFakeDataSetStreamSupplier(long maxSize, long seed) {
		this.maxSize = maxSize;
		this.seed = seed;
		reset();
	}

	public void reset() {
		random = new Random(seed);
	}

	public Iterator<TripleString> createTripleStringStream() throws IOException {
		return RDFStreamUtils.readRDFStreamAsTripleStringIterator(createRDFStream(RDFFormat.NTRIPLES),
				RDFFormat.NTRIPLES, false);
	}

	public InputStream createRDFStream(RDFFormat format) throws IOException {
		PipedInputStream in = new PipedInputStream();
		PipedOutputStream out = new PipedOutputStream();
		in.connect(out);
		RDFWriter writer = Rio.createWriter(format, out);
		Thread loaded = new Thread(() -> {
			writer.startRDF();
			FakeStatementIterator it = new FakeStatementIterator();
			while (it.hasNext()) {
				writer.handleStatement(it.next());
			}
			writer.endRDF();
			try {
				out.close();
			} catch (IOException e) {
				// ignore
			}
		}, "FakeDatasetStreamLoader");
		loaded.start();
		return in;
	}

	private Resource createSubject() {
		return createPredicate();
	}

	private IRI createPredicate() {
		return valueFactory.createIRI("http://w" + random.nextInt() + "i.test.org/#Obj" + random.nextInt());
	}

	private IRI createType() {
		return valueFactory.createIRI("http://wti.test.org/#Obj" + random.nextInt(maxFakeType));
	}

	private Value createValue() {
		if (random.nextBoolean()) {
			return createPredicate();
		}

		String text = MultiLanguageTypedLuceneIndexTest.stringNameOfInt(random.nextInt(Integer.MAX_VALUE));
		if (random.nextBoolean()) {
			return valueFactory.createLiteral(text,
					MultiLanguageTypedLuceneIndexTest.stringNameOfInt(random.nextInt(Integer.MAX_VALUE)));
		} else {
			return valueFactory.createLiteral(text, createType());
		}
	}

	private class FakeStatementIterator implements Iterator<Statement> {
		private long size;
		private Statement next;

		@Override
		public boolean hasNext() {
			if (size >= maxSize) {
				return false;
			}
			if (next != null) {
				return true;
			}

			next = valueFactory.createStatement(createSubject(), createPredicate(), createValue());

			long estimation = FileTripleIterator.estimateTripleSize(new TripleString(next.getSubject().toString(),
					next.getPredicate().toString(), next.getObject().toString()));
			size += estimation;

			return size < maxSize;
		}

		@Override
		public Statement next() {
			if (!hasNext()) {
				return null;
			}
			Statement next = this.next;
			this.next = null;
			return next;
		}
	}

}
