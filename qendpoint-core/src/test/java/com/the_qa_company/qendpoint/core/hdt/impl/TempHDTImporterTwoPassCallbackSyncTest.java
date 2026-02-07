package com.the_qa_company.qendpoint.core.hdt.impl;

import com.the_qa_company.qendpoint.core.triples.TripleString;
import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import static org.junit.Assert.assertTrue;

public class TempHDTImporterTwoPassCallbackSyncTest {
	@Test
	public void dictionaryAppenderProcessTripleIsSynchronized() throws Exception {
		Method method = TempHDTImporterTwoPass.DictionaryAppender.class.getMethod("processTriple", TripleString.class,
				long.class);
		assertTrue("DictionaryAppender.processTriple must be synchronized for parallel RIOT parse",
				Modifier.isSynchronized(method.getModifiers()));
	}

	@Test
	public void tripleAppender2ProcessTripleIsSynchronized() throws Exception {
		Method method = TempHDTImporterTwoPass.TripleAppender2.class.getMethod("processTriple", TripleString.class,
				long.class);
		assertTrue("TripleAppender2.processTriple must be synchronized for parallel RIOT parse",
				Modifier.isSynchronized(method.getModifiers()));
	}
}
