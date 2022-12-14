package com.the_qa_company.qendpoint.controller;

import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;

import static org.junit.Assert.*;

public class SparqlTest {

	@Test
	public void getFileNameTest() throws MalformedURLException {
		assertEquals("test.rdf", Sparql.getFileName(new URL("http://example.org/test.rdf")));
		assertEquals("test.rdf", Sparql.getFileName(new URL("http://example.org/a/b/test.rdf")));
		assertEquals("test.rdf", Sparql.getFileName(new URL("http://example.org/test.rdf?a=2&b=3")));
		assertEquals("test.rdf", Sparql.getFileName(new URL("http://example.org/a/b/test.rdf?a=2&b=3")));
		assertEquals("test.rdf", Sparql.getFileName(new URL("http://example.org/a/b/test.rdf#/hello")));
		assertEquals("test.rdf.gz", Sparql.getFileName(new URL("http://example.org/a/b/test.rdf.gz")));
		assertEquals("", Sparql.getFileName(new URL("http://example.org/")));
		assertEquals("", Sparql.getFileName(new URL("http://example.org")));
	}
}
