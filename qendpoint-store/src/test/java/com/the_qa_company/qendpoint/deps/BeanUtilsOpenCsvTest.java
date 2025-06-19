package com.the_qa_company.qendpoint.deps;

import com.the_qa_company.qendpoint.core.iterator.utils.FetcherIterator;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.query.AbstractTupleQueryResultHandler;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.query.resultio.text.tsv.SPARQLResultsTSVParser;
import org.eclipse.rdf4j.query.resultio.text.tsv.SPARQLResultsTSVWriter;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BeanUtilsOpenCsvTest {
	public static class BindingGenerator extends FetcherIterator<BindingSet> {
		private final Random rndo;
		private long remaining;
		private final String[] bindingNames;

		public BindingGenerator(long remaining, String[] bindingNames, long seed) {
			this.remaining = remaining;
			this.bindingNames = bindingNames;
			this.rndo = new Random(seed);
		}

		@Override
		protected BindingSet getNext() {
			if (remaining <= 0) {
				return null;
			}
			remaining--;

			MapBindingSet set = new MapBindingSet();
			for (String keyname : bindingNames) {
				Value v = switch (rndo.nextInt(4)) {
				case 0 -> Values.bnode("bn" + rndo.nextInt());
				case 1 -> Values.literal(rndo.nextInt());
				case 2 -> Values.literal("v" + rndo.nextInt());
				case 3 -> Values.iri("http://example.org/#l" + rndo.nextInt());
				default -> throw new AssertionError();
				};
				set.addBinding(keyname, v);
			}
			return set;
		}
	}

	/*
	 * This test is here because opencsv is using beans-utils 1.9.4, but we want
	 * 1.11.0 because the current version has a security issue.
	 */
	@Test
	public void testCsv() {
		final int seed = 58;
		final int count = 10000;

		ByteArrayOutputStream os = new ByteArrayOutputStream();

		SPARQLResultsTSVWriter writer = new SPARQLResultsTSVWriter(os);
		writer.startHeader();
		writer.endHeader();
		writer.startDocument();
		String[] bindingNames = { "aaa", "bbb", "ccc", "ddd", "eee", "fff", "v000", "v111" };
		writer.startQueryResult(List.of(bindingNames));

		BindingGenerator gen = new BindingGenerator(count, bindingNames, seed);
		gen.forEachRemaining(writer::handleSolution);

		writer.endQueryResult();

		SPARQLResultsTSVParser reader = new SPARQLResultsTSVParser();

		BindingGenerator genin = new BindingGenerator(count, bindingNames, seed);

		reader.setQueryResultHandler(new AbstractTupleQueryResultHandler() {
			@Override
			public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {
				assertTrue(genin.hasNext());
				assertEquals(genin.next(), bindingSet);
			}
		});
		reader.set(BasicParserSettings.PRESERVE_BNODE_IDS, true);
		reader.parse(new ByteArrayInputStream(os.toByteArray()));
		assertFalse(genin.hasNext());

	}
}
