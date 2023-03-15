package com.the_qa_company;

import org.apache.commons.io.file.PathUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Benchmark class to test the endpoint
 *
 * @author Antoine Willerval
 */
@State(Scope.Benchmark)
public class EndpointBenchmark {

	Path root;
	Path dataset;

	@Setup
	public void setup() throws IOException {
		root = Files.createTempDirectory("qepbench");

		try {
			dataset = root.resolve("dataset.nt");
			dataset.toFile().deleteOnExit();


		} catch (Throwable t) {
			try {
				PathUtils.deleteDirectory(root);
			} catch (Throwable t2) {
				t.addSuppressed(t2);
			}
			throw t;
		}
	}

	@TearDown
	public void complete() throws IOException {
		PathUtils.deleteDirectory(root);
	}

	@Benchmark
	public void endpoint() {

	}

}
