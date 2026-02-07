package com.the_qa_company.qendpoint.core.tools;

import com.beust.jcommander.JCommander;
import com.the_qa_company.qendpoint.core.hdt.HDTVersion;
import com.the_qa_company.qendpoint.core.util.listener.ColorTool;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RDF2HDTNoRecreateTest {

	@Rule
	public TemporaryFolder temp = new TemporaryFolder();

	@Test
	public void noRecreateSkipsConversionAndBuildsIndex() throws Throwable {
		Path input = temp.newFile("input.nt").toPath();
		Path output = temp.getRoot().toPath().resolve("output.hdt");
		Files.writeString(input, "<http://ex.org/s> <http://ex.org/p> <http://ex.org/o> .\n");

		runRdf2Hdt("-quiet", input.toString(), output.toString());

		assertTrue(Files.exists(output));

		Path index = Path.of(output.toString() + HDTVersion.get_index_suffix("-"));
		assertFalse(Files.exists(index));

		FileTime before = Files.getLastModifiedTime(output);

		runRdf2Hdt("-quiet", "-index", "-norecreate", input.toString(), output.toString());

		assertTrue(Files.exists(index));
		assertEquals(before, Files.getLastModifiedTime(output));
	}

	private void runRdf2Hdt(String... args) throws Throwable {
		RDF2HDT rdf2hdt = new RDF2HDT();
		JCommander com = new JCommander(rdf2hdt);
		com.parse(args);
		if (rdf2hdt.parameters.size() == 1) {
			rdf2hdt.rdfInput = "-";
			rdf2hdt.hdtOutput = rdf2hdt.parameters.get(0);
		} else if (rdf2hdt.parameters.size() == 2) {
			rdf2hdt.rdfInput = rdf2hdt.parameters.get(0);
			rdf2hdt.hdtOutput = rdf2hdt.parameters.get(1);
		} else {
			throw new IllegalArgumentException("Expected <input RDF> <output HDT> parameters.");
		}
		Field colorField = RDF2HDT.class.getDeclaredField("colorTool");
		colorField.setAccessible(true);
		colorField.set(rdf2hdt, new ColorTool(rdf2hdt.color, rdf2hdt.quiet));
		rdf2hdt.execute();
	}
}
