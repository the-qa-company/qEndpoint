package com.the_qa_company.q_endpoint.utils.sail.builder;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.NotifyingSail;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class SailCompilerTest {
	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	private void loadFile(String fileName) throws IOException, SailCompiler.SailCompilerException {
		String locationNative = tempDir.newFolder().getAbsolutePath();
		SailCompiler compiler = new SailCompiler();

		try (InputStream is = getClass().getClassLoader().getResourceAsStream(fileName)) {
			compiler.load(is, Rio.getParserFormatForFileName(fileName).orElseThrow());
		}

		compiler.registerDirString("locationNative", locationNative);

		MemoryStore source = new MemoryStore();
		NotifyingSail compiledSail = compiler.compile(source);


		ValueFactory vf = SimpleValueFactory.getInstance();

		SailRepository repo = new SailRepository(compiledSail);
		repo.init();
		try (SailRepositoryConnection connection = repo.getConnection()) {
			connection.add(vf.createStatement(vf.createIRI("http://aaa"), vf.createIRI("http://bbb"), vf.createIRI("http://ccc")));
		}
	}

	@Test
	public void loadModel1Test() throws IOException, SailCompiler.SailCompilerException {
		loadFile("model/model_example1.ttl");
	}

	@Test
	public void loadModel2Test() throws IOException, SailCompiler.SailCompilerException {
		loadFile("model/model_example2.ttl");
	}

	@Test
	public void dirCompileTest() throws SailCompiler.SailCompilerException {
		SailCompiler compiler = new SailCompiler();
		compiler.registerDirString("myKey", "my cat");
		Assert.assertEquals("Dir string not parsed", "I love my cat", compiler.parseDir("I love ${myKey}"));
	}
}
