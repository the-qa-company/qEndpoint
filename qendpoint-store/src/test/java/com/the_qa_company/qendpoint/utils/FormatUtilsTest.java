package com.the_qa_company.qendpoint.utils;

import org.eclipse.rdf4j.query.resultio.QueryResultFormat;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultFormat;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.Test;

import static org.junit.Assert.*;

public class FormatUtilsTest {

	@Test
	public void getResultWriterFormat() {
		assertEquals(TupleQueryResultFormat.JSON,
				FormatUtils
						.getResultWriterFormat(
								"text/csv;q=0.8,application/sparql-results+json,text/tab-separated-values;q=0.8")
						.orElseThrow());
		assertEquals(TupleQueryResultFormat.TSV,
				FormatUtils
						.getResultWriterFormat(
								"text/csv;q=0.8,application/sparql-results+json;q=0.8,text/tab-separated-values;q=0.9")
						.orElseThrow());
		assertEquals(TupleQueryResultFormat.CSV,
				FormatUtils
						.getResultWriterFormat(
								"text/csv,application/sparql-results+json;q=0.8,text/tab-separated-values;q=0.9")
						.orElseThrow());
	}

	@Test
	public void getRDFWriterFormat() {
		assertEquals(RDFFormat.RDFJSON, FormatUtils
				.getRDFWriterFormat("text/turtle;q=0.8,application/rdf+json,application/trig;q=0.8").orElseThrow());
		assertEquals(RDFFormat.TRIG,
				FormatUtils.getRDFWriterFormat("text/turtle;q=0.8,application/rdf+json;q=0.8,application/trig;q=0.9")
						.orElseThrow());
		assertEquals(RDFFormat.TURTLE, FormatUtils
				.getRDFWriterFormat("text/turtle,application/rdf+json;q=0.8,application/trig;q=0.9").orElseThrow());
	}
}
