package com.the_qa_company.qendpoint.core.triples.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.ControlInformation;
import com.the_qa_company.qendpoint.core.options.HDTSpecification;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TriplesListLongRoundTripTest {

	@Test
	public void roundTripPreservesTriples() throws IOException {
		TriplesListLong triples = new TriplesListLong(new HDTSpecification());
		triples.insert(1L, 2L, 3L);

		ControlInformation controlInformation = new ControlInformation();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		triples.save(out, controlInformation, ProgressListener.ignore());

		ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
		ControlInformation loadedControl = new ControlInformation();
		loadedControl.load(in);

		TriplesListLong loaded = new TriplesListLong(new HDTSpecification());
		loaded.load(in, loadedControl, ProgressListener.ignore());

		assertEquals(1L, loaded.getNumberOfElements());
		IteratorTripleID iterator = loaded.searchAll();
		assertTrue(iterator.hasNext());
		TripleID triple = iterator.next();
		assertEquals(1L, triple.getSubject());
		assertEquals(2L, triple.getPredicate());
		assertEquals(3L, triple.getObject());
		assertFalse(iterator.hasNext());
	}
}
