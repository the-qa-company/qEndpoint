package com.the_qa_company.qendpoint.store;

import org.junit.Test;

import java.nio.file.Path;

import static org.junit.Assert.*;

public class EndpointStoreDumpTest {

	@Test
	public void nameTest() {
		assertEquals("aaaa.hdt",
				EndpointStoreDump.EndpointStoreDumpDataset.replaceHDTFilename(Path.of("test.hdt"), "aaaa"));
		assertEquals("bbbb.hdt",
				EndpointStoreDump.EndpointStoreDumpDataset.replaceHDTFilename(Path.of("test.hdt"), "bbbb"));
		assertEquals("bbbb.hdt.idx",
				EndpointStoreDump.EndpointStoreDumpDataset.replaceHDTFilename(Path.of("test.hdt.idx"), "bbbb"));
		assertEquals("bbbb.hdt",
				EndpointStoreDump.EndpointStoreDumpDataset.replaceHDTFilename(Path.of("test.idx.hdt"), "bbbb"));
	}
}
