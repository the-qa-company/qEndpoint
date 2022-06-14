package com.the_qa_company.qendpoint.controller;

import com.the_qa_company.qendpoint.Application;
import com.the_qa_company.qendpoint.compiler.DebugOptionTestUtils;
import com.the_qa_company.qendpoint.compiler.SailCompilerSchema;
import org.eclipse.rdf4j.model.IRI;
import org.junit.After;
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
		return SailCompilerSchema.STORAGE_MODE_PROPERTY.getHandler().getValues().stream().map(iri -> (Object) iri)
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

	private Runnable storageModeUnset;

	private final IRI mode;

	public TripleSourceTest(IRI mode) {
		this.mode = mode;
	}

	@Before
	public void setup() throws Exception {
		// init spring runner
		TestContextManager testContextManager = new TestContextManager(getClass());
		testContextManager.prepareTestInstance(this);

		// clear map to recreate endpoint store
		sparql.init = false;
		storageModeUnset = DebugOptionTestUtils.setStorageMode(mode);

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

	@After
	public void complete() {
		storageModeUnset.run();
	}

	@Test
	public void optimizedTest() throws IOException {
		Runnable op = DebugOptionTestUtils.setOptimization(true);

		sparql.shutdown();
		sparql.initializeEndpointStore(true);

		op.run();
	}

	@Test
	public void noOptimizedTest() throws IOException {
		Runnable op = DebugOptionTestUtils.setOptimization(false);

		sparql.shutdown();
		sparql.initializeEndpointStore(true);

		op.run();
	}
}
