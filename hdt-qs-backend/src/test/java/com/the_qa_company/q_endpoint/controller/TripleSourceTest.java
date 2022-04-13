package com.the_qa_company.q_endpoint.controller;

import com.the_qa_company.q_endpoint.Application;
import com.the_qa_company.q_endpoint.utils.sail.builder.SailCompilerSchema;
import org.eclipse.rdf4j.model.IRI;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestContextManager;
import org.springframework.util.FileSystemUtils;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
@ContextConfiguration(initializers = ConfigFileApplicationContextInitializer.class)
@SpringBootTest(classes = Application.class)
public class TripleSourceTest {
	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object> params() {
		return SailCompilerSchema.STORAGE_MODE_PROPERTY.getValues().stream()
				.map(iri -> (Object) iri)
				.collect(Collectors.toList());
	}

	@Autowired
	Sparql sparql;

	@Value("${locationHdt}")
	String locationHdt;

	@Value("${hdtIndexName}")
	String hdtIndexName;

	@Value("${locationNative}")
	String locationNative;

	private final IRI mode;
	public TripleSourceTest(IRI mode) {
		this.mode = mode;
	}
	@Before
	public void setup() throws Exception {
		// init spring runner
		TestContextManager testContextManager = new TestContextManager(getClass());
		testContextManager.prepareTestInstance(this);

		// clear map to recreate hybrid store
		sparql.model.clear();
		sparql.options.storageMode = mode;

		// remove previous data
		try {
			FileSystemUtils.deleteRecursively(Paths.get(locationHdt));
		} catch (IOException e) {
			//
		}
		try {
			FileSystemUtils.deleteRecursively(Paths.get(locationNative));
		} catch (IOException e) {
			//
		}
		try {
			FileSystemUtils.deleteRecursively(Paths.get(hdtIndexName));
		} catch (IOException e) {
			//
		}
	}

	@Test
	public void optimizedTest() throws IOException {
		sparql.clearHybridStore(locationHdt);
		sparql.initializeHybridStore(locationHdt, true);
	}
	@Test
	public void noOptimizedTest() throws IOException {
		sparql.options.optimization = false;

		sparql.clearHybridStore(locationHdt);
		sparql.initializeHybridStore(locationHdt, true);

		sparql.options.optimization = true;
	}
}
