package com.the_qa_company.qendpoint.model;

import com.the_qa_company.qendpoint.store.Utility;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.options.HDTSpecification;

import java.io.IOException;

public class IRITest {

	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	HDTSpecification spec;

	@Before
	public void setUp() {
		spec = new HDTSpecification();
		spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");
	}

	@Test
	public void equality() throws IOException {
		ValueFactory factory = SimpleValueFactory.getInstance();
		System.out.println(tempDir.getRoot().getAbsolutePath());
		HDT hdt = Utility.createTempHdtIndex(tempDir, false, false, spec);
		SimpleIRIHDT s1 = new SimpleIRIHDT(hdt, "http://s1");
		IRI s2 = factory.createIRI("http://s1");

		Assert.assertEquals(s1.hashCode(), s2.hashCode());
		Assert.assertEquals(s1.getLocalName(), s2.getLocalName());
		Assert.assertEquals(s1.getNamespace(), s2.getNamespace());
		Assert.assertEquals(s1.isIRI(), s2.isIRI());
	}

}
