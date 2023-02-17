package com.the_qa_company.qendpoint.controller;

import com.the_qa_company.qendpoint.core.header.HeaderUtil;
import com.the_qa_company.qendpoint.core.header.PlainHeader;
import com.the_qa_company.qendpoint.core.util.UnicodeEscape;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SparqlTest {

	private void uriTest(String uri) {
		String baseURI = Sparql.baseURIFromFilename(uri);
		String baseURINT = "<" + baseURI + ">";

		PlainHeader header = new PlainHeader();

		header.insert(baseURI, "<p>", "\"thing\"");
		header.insert(baseURI, HeaderUtil.cleanURI("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"),
				HeaderUtil.cleanURI("<http://purl.org/HDT/hdt#Dataset>"));

		assertEquals(header.search(baseURI, "<p>", "").next().getObject(), "\"thing\"");

		UnicodeEscape.unescapeString(baseURI);

		assertEquals(baseURI, header.getBaseURI());
	}

	@Test
	public void baseURIWindowsTest() {
		uriTest("c:\\test\\test.rdf");
	}

	@Test
	public void baseURILinuxTest() {
		uriTest("/usr/test/test.rdf");
	}
}
