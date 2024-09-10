package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriplesIndexFile;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.testsuite.query.parser.sparql.manifest.SPARQL11QueryComplianceTest;
import org.eclipse.rdf4j.testsuite.query.parser.sparql.manifest.SPARQL11UpdateComplianceTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

public class ComplianceTest {
	private static final Logger logger = LoggerFactory.getLogger(ComplianceTest.class);

	@TempDir
	public Path tempDir;

	public class EndpointMultIndexSPARQL11QueryComplianceTest extends SPARQL11QueryComplianceTest {

		public EndpointMultIndexSPARQL11QueryComplianceTest() {
			super();
			List<String> testToIgnore = new ArrayList<>();
			// @todo these tests are failing and should not, they are skipped so
			// that we can be sure that we see when
			// currently passing tests are not failing. Many of these tests are
			// not
			// so problematic since we do not support
			// named graphs anyway
			testToIgnore.add("STRDT() TypeErrors");
			testToIgnore.add("STRLANG() TypeErrors");
			testToIgnore.add("constructwhere02 - CONSTRUCT WHERE");
			testToIgnore.add("constructwhere03 - CONSTRUCT WHERE");
			testToIgnore.add("constructwhere04 - CONSTRUCT WHERE");
			testToIgnore.add("Exists within graph pattern");
			testToIgnore.add("(pp07) Path with one graph");
			testToIgnore.add("(pp35) Named Graph 2");
			testToIgnore.add("sq01 - Subquery within graph pattern");
			testToIgnore.add("sq02 - Subquery within graph pattern, graph variable is bound");
			testToIgnore.add("sq03 - Subquery within graph pattern, graph variable is not bound");
			testToIgnore.add("sq04 - Subquery within graph pattern, default graph does not apply");
			testToIgnore.add("sq05 - Subquery within graph pattern, from named applies");
			testToIgnore.add("sq06 - Subquery with graph pattern, from named applies");
			testToIgnore.add("sq07 - Subquery with from ");
			testToIgnore.add("sq11 - Subquery limit per resource");
			testToIgnore.add("sq13 - Subqueries don't inject bindings");
			testToIgnore.add("sq14 - limit by resource");

			this.setIgnoredTests(testToIgnore);
		}

		EndpointStore endpoint;
		File nativeStore;
		File hdtStore;

		@Override
		protected void testParameterListener(String displayName, String testURI, String name, String queryFileURL,
				String resultFileURL, Dataset dataset, boolean ordered, boolean laxCardinality) {
			try {
				setUpHDT(dataset);
			} catch (IOException | ParserException | NotFoundException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		protected Repository newRepository() throws Exception {
			Path localTemp = tempDir.resolve(UUID.randomUUID().toString());
			Files.createDirectories(localTemp);

			nativeStore = localTemp.resolve("native").toFile();
			Files.createDirectories(nativeStore.toPath());
			hdtStore = localTemp.resolve("hdt").toFile();
			Files.createDirectories(hdtStore.toPath());

			HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
					HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS, HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS,
					EnumSet.of(TripleComponentOrder.SPO, TripleComponentOrder.OPS, TripleComponentOrder.PSO));
			Path fileName = Path.of(hdtStore.getAbsolutePath() + "/" + EndpointStoreTest.HDT_INDEX_NAME);
			if (this.hdt == null) {
				hdt = Utility.createTempHdtIndex(localTemp, true, false, spec);
			}
			assert hdt != null;

			hdt.saveToHDT(fileName, null);

			endpoint = new EndpointStore(hdtStore.getAbsolutePath() + "/", EndpointStoreTest.HDT_INDEX_NAME, spec,
					nativeStore.getAbsolutePath() + "/", true) {
				@Override
				public HDT loadIndex() throws IOException {
					HDT idx = super.loadIndex();
					if (idx.getTriples().getNumberOfElements() == 0) {
						return idx;
					}
					try {
						Path fileOPS = BitmapTriplesIndexFile.getIndexPath(fileName, TripleComponentOrder.OPS);
						Assertions.assertTrue(Files.exists(fileOPS), "can't find " + fileOPS);
						Path filePSO = BitmapTriplesIndexFile.getIndexPath(fileName, TripleComponentOrder.PSO);
						Assertions.assertTrue(Files.exists(filePSO), "can't find " + filePSO);
					} catch (Throwable t) {
						try (Stream<Path> l = Files.list(fileName.getParent())) {
							l.forEach(System.err::println);
						} catch (Exception e) {
							t.addSuppressed(e);
						}
						throw t;
					}
					return idx;
				}
			};
			// endpoint.setThreshold(2);
			return new SailRepository(endpoint);
		}

		HDT hdt;

		private void setUpHDT(Dataset dataset) throws IOException, ParserException, NotFoundException {
			if (dataset == null) {
				return;
			}

			String x = dataset.getDefaultGraphs().toString();
			if (x.equals("[]")) {
				x = dataset.getNamedGraphs().toString();
			}
			String str = x.substring(x.lastIndexOf("!") + 1).replace("]", "");

			URL url = SPARQL11QueryComplianceTest.class.getResource(str);
			File tmpDir = new File("test");
			if (tmpDir.mkdirs()) {
				logger.debug("{} dir created.", tmpDir);
			}
			assert url != null;
			JarURLConnection con = (JarURLConnection) url.openConnection();
			File file = new File(tmpDir, con.getEntryName());

			HDTOptions spec = HDTOptions.of();

			hdt = HDTManager.generateHDT(file.getAbsolutePath(), "http://www.example.org/", RDFNotation.guess(file),
					spec, null);
			assert hdt != null;
			hdt.search("", "", "").forEachRemaining(System.out::println);
		}
	}

	public class EndpointMultIndexSPARQL11UpdateComplianceTest extends SPARQL11UpdateComplianceTest {

		public EndpointMultIndexSPARQL11UpdateComplianceTest() {
			super();
			List<String> testToIgnore = new ArrayList<>();
			// @todo these tests are failing and should not, they are skipped so
			// that we can be sure that we see when
			// currently passing tests are not failing. Many of these tests are
			// not
			// so problematic since we do not support
			// named graphs anyway
			testToIgnore.add("DELETE INSERT 1b");
			testToIgnore.add("DELETE INSERT 1c");
			testToIgnore.add("CLEAR NAMED");
			testToIgnore.add("DROP NAMED");
			this.setIgnoredTests(testToIgnore);
		}

		@Override
		protected Repository newRepository() throws Exception {

			Path localTemp = tempDir.resolve(UUID.randomUUID().toString());
			Files.createDirectories(localTemp);

			File nativeStore = localTemp.resolve("native").toFile();
			Files.createDirectories(nativeStore.toPath());
			File hdtStore = localTemp.resolve("hdt").toFile();
			Files.createDirectories(hdtStore.toPath());

			HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
					HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS, HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS,
					EnumSet.of(TripleComponentOrder.SPO, TripleComponentOrder.OPS, TripleComponentOrder.PSO));
			Path fileName = Path.of(hdtStore.getAbsolutePath() + "/" + EndpointStoreTest.HDT_INDEX_NAME);
			try (HDT hdt = Utility.createTempHdtIndex(localTemp, true, false, spec)) {
				assert hdt != null;
				hdt.saveToHDT(fileName, null);
			}

			EndpointStore endpoint = new EndpointStore(hdtStore.getAbsolutePath() + "/",
					EndpointStoreTest.HDT_INDEX_NAME, spec, nativeStore.getAbsolutePath() + "/", true) {

				@Override
				public HDT loadIndex() throws IOException {
					HDT idx = super.loadIndex();
					if (idx.getTriples().getNumberOfElements() == 0) {
						return idx;
					}
					try {
						Path fileOPS = BitmapTriplesIndexFile.getIndexPath(fileName, TripleComponentOrder.OPS);
						Assertions.assertTrue(Files.exists(fileOPS), "can't find " + fileOPS);
						Path filePSO = BitmapTriplesIndexFile.getIndexPath(fileName, TripleComponentOrder.PSO);
						Assertions.assertTrue(Files.exists(filePSO), "can't find " + filePSO);
					} catch (Throwable t) {
						try (Stream<Path> l = Files.list(fileName.getParent())) {
							l.forEach(System.err::println);
						} catch (Exception e) {
							t.addSuppressed(e);
						}
						throw t;
					}
					return idx;
				}
			};
			// endpoint.setThreshold(2);

			return new SailRepository(endpoint);
		}

	}

	public class EndpointSPARQL11QueryComplianceTest extends SPARQL11QueryComplianceTest {
		private static final Logger logger = LoggerFactory.getLogger(EndpointSPARQL11QueryComplianceTest.class);

		public EndpointSPARQL11QueryComplianceTest() {
			super();

			List<String> testToIgnore = Arrays.asList(
					// @todo these tests are failing and should not, they are
					// skipped so
					// that we can be sure that we see when
					// currently passing tests are not failing. Many of these
					// tests
					// are not
					// so problematic since we do not support
					// named graphs anyway
					"STRDT() TypeErrors", "STRLANG() TypeErrors", "constructwhere02 - CONSTRUCT WHERE",
					"constructwhere03 - CONSTRUCT WHERE", "constructwhere04 - CONSTRUCT WHERE",
					"Exists within graph pattern", "(pp07) Path with one graph", "(pp35) Named Graph 2",
					"sq01 - Subquery within graph pattern",
					"sq02 - Subquery within graph pattern, graph variable is bound",
					"sq03 - Subquery within graph pattern, graph variable is not bound",
					"sq04 - Subquery within graph pattern, default graph does not apply",
					"sq05 - Subquery within graph pattern, from named applies",
					"sq06 - Subquery with graph pattern, from named applies", "sq07 - Subquery with from ",
					"sq11 - Subquery limit per resource", "sq13 - Subqueries don't inject bindings",
					"sq14 - limit by resource");

			this.setIgnoredTests(testToIgnore);
		}

		@Override
		protected void testParameterListener(String displayName, String testURI, String name, String queryFileURL,
				String resultFileURL, Dataset dataset, boolean ordered, boolean laxCardinality) {
			try {
				setUpHDT(dataset);
			} catch (IOException | ParserException | NotFoundException e) {
				throw new RuntimeException(e);
			}
		}

		EndpointStore endpoint;
		File nativeStore;
		File hdtStore;

		@Override
		protected Repository newRepository() throws Exception {
			Path localTemp = tempDir.resolve(UUID.randomUUID().toString());
			Files.createDirectories(localTemp);

			nativeStore = localTemp.resolve("native").toFile();
			Files.createDirectories(nativeStore.toPath());
			hdtStore = localTemp.resolve("hdt").toFile();
			Files.createDirectories(hdtStore.toPath());

			HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
					HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS);
			if (this.hdt == null) {
				hdt = Utility.createTempHdtIndex(localTemp, true, false, spec);
			}
			assert hdt != null;

			hdt.saveToHDT(hdtStore.getAbsolutePath() + "/" + EndpointStoreTest.HDT_INDEX_NAME, null);

			endpoint = new EndpointStore(hdtStore.getAbsolutePath() + "/", EndpointStoreTest.HDT_INDEX_NAME, spec,
					nativeStore.getAbsolutePath() + "/", true);
			// endpoint.setThreshold(2);
			return new SailRepository(endpoint);
		}

		HDT hdt;

		private void setUpHDT(Dataset dataset) throws IOException, ParserException, NotFoundException {
			if (dataset == null) {
				return;
			}

			String x = dataset.getDefaultGraphs().toString();
			if (x.equals("[]")) {
				x = dataset.getNamedGraphs().toString();
			}
			String str = x.substring(x.lastIndexOf("!") + 1).replace("]", "");

			URL url = SPARQL11QueryComplianceTest.class.getResource(str);
			File tmpDir = new File("test");
			if (tmpDir.mkdirs()) {
				logger.debug("{} dir created.", tmpDir);
			}
			assert url != null;
			JarURLConnection con = (JarURLConnection) url.openConnection();
			File file = new File(tmpDir, con.getEntryName());

			HDTOptions spec = HDTOptions.of();

			hdt = HDTManager.generateHDT(file.getAbsolutePath(), "http://www.example.org/", RDFNotation.guess(file),
					spec, null);
			assert hdt != null;
			hdt.search("", "", "").forEachRemaining(System.out::println);
		}
	}

	public class EndpointSPARQL11UpdateComplianceTest extends SPARQL11UpdateComplianceTest {

		public EndpointSPARQL11UpdateComplianceTest() {

			List<String> testToIgnore = new ArrayList<>();
			// @todo these tests are failing and should not, they are skipped so
			// that we can be sure that we see when
			// currently passing tests are not failing. Many of these tests are
			// not
			// so problematic since we do not support
			// named graphs anyway
			testToIgnore.add("DELETE INSERT 1b");
			testToIgnore.add("DELETE INSERT 1c");
			testToIgnore.add("CLEAR NAMED");
			testToIgnore.add("DROP NAMED");
			this.setIgnoredTests(testToIgnore);
		}

		@Override
		protected Repository newRepository() throws Exception {
			Path localTemp = tempDir.resolve(UUID.randomUUID().toString());
			Files.createDirectories(localTemp);

			File nativeStore = localTemp.resolve("native").toFile();
			Files.createDirectories(nativeStore.toPath());
			File hdtStore = localTemp.resolve("hdt").toFile();
			Files.createDirectories(hdtStore.toPath());

			HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
					HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS);
			try (HDT hdt = Utility.createTempHdtIndex(localTemp, true, false, spec)) {
				assert hdt != null;
				hdt.saveToHDT(hdtStore.getAbsolutePath() + "/" + EndpointStoreTest.HDT_INDEX_NAME, null);
			}

			EndpointStore endpoint = new EndpointStore(hdtStore.getAbsolutePath() + "/",
					EndpointStoreTest.HDT_INDEX_NAME, spec, nativeStore.getAbsolutePath() + "/", true);
			// endpoint.setThreshold(2);

			return new SailRepository(endpoint);
			// return new DatasetRepository(new SailRepository(new
			// NativeStore(tempDir.newFolder(), "spoc")));
		}

	}

	public class EndpointQuadSPARQL11QueryComplianceTest extends SPARQL11QueryComplianceTest {
		private static final Logger logger = LoggerFactory.getLogger(EndpointSPARQL11QueryComplianceTest.class);

		@Override
		protected void testParameterListener(String displayName, String testURI, String name, String queryFileURL,
				String resultFileURL, Dataset dataset, boolean ordered, boolean laxCardinality) {
			try {
				setUpHDT(dataset);
			} catch (IOException | ParserException | NotFoundException e) {
				throw new RuntimeException(e);
			}
		}

		EndpointStore endpoint;
		File nativeStore;
		File hdtStore;

		@Override
		protected Repository newRepository() throws Exception {
			Path localTemp = tempDir.resolve(UUID.randomUUID().toString());
			Files.createDirectories(localTemp);

			nativeStore = localTemp.resolve("native").toFile();
			Files.createDirectories(nativeStore.toPath());
			hdtStore = localTemp.resolve("hdt").toFile();
			Files.createDirectories(hdtStore.toPath());

			HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
					HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG_QUAD);
			if (this.hdt == null) {
				hdt = Utility.createTempHdtIndex(localTemp, true, false, spec);
			}
			assert hdt != null;

			hdt.saveToHDT(hdtStore.getAbsolutePath() + "/" + EndpointStoreTest.HDT_INDEX_NAME, null);

			endpoint = new EndpointStore(hdtStore.getAbsolutePath() + "/", EndpointStoreTest.HDT_INDEX_NAME, spec,
					nativeStore.getAbsolutePath() + "/", true);
			// endpoint.setThreshold(2);
			return new SailRepository(endpoint);
		}

		HDT hdt;

		private void setUpHDT(Dataset dataset) throws IOException, ParserException, NotFoundException {
			if (dataset == null) {
				return;
			}

			String x = dataset.getDefaultGraphs().toString();
			if (x.equals("[]")) {
				x = dataset.getNamedGraphs().toString();
			}
			String str = x.substring(x.lastIndexOf("!") + 1).replace("]", "");

			URL url = SPARQL11QueryComplianceTest.class.getResource(str);
			File tmpDir = new File("test");
			if (tmpDir.mkdirs()) {
				logger.debug("{} dir created.", tmpDir);
			}
			assert url != null;
			JarURLConnection con = (JarURLConnection) url.openConnection();
			File file = new File(tmpDir, con.getEntryName());

			HDTOptions spec = HDTOptions.of();

			hdt = HDTManager.generateHDT(file.getAbsolutePath(), "http://www.example.org/", RDFNotation.guess(file),
					spec, null);
			assert hdt != null;
			hdt.search("", "", "").forEachRemaining(System.out::println);
		}
	}

	public class EndpointQuadSPARQL11UpdateComplianceTest extends SPARQL11UpdateComplianceTest {

		@Override
		protected Repository newRepository() throws Exception {
			Path localTemp = tempDir.resolve(UUID.randomUUID().toString());
			Files.createDirectories(localTemp);

			File nativeStore = localTemp.resolve("native").toFile();
			Files.createDirectories(nativeStore.toPath());
			File hdtStore = localTemp.resolve("hdt").toFile();
			Files.createDirectories(hdtStore.toPath());

			HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
					HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG_QUAD);
			try (HDT hdt = Utility.createTempHdtIndex(localTemp, true, false, spec)) {
				assert hdt != null;
				hdt.saveToHDT(hdtStore.getAbsolutePath() + "/" + EndpointStoreTest.HDT_INDEX_NAME, null);
			}

			EndpointStore endpoint = new EndpointStore(hdtStore.getAbsolutePath() + "/",
					EndpointStoreTest.HDT_INDEX_NAME, spec, nativeStore.getAbsolutePath() + "/", true);

			return new SailRepository(endpoint);
		}

	}

	@TestFactory
	public Collection<DynamicTest> multiIndexQuery() {
		return new EndpointMultIndexSPARQL11QueryComplianceTest().tests();
	}

	@TestFactory
	public Collection<DynamicTest> indexQuery() {
		return new EndpointSPARQL11QueryComplianceTest().tests();
	}

	/*
	 * @TestFactory public Collection<DynamicTest> multiIndexUpdate() { return
	 * new EndpointMultIndexSPARQL11UpdateComplianceTest().getTestData(); }
	 * @TestFactory public Collection<DynamicTest> indexUpdate() { return new
	 * EndpointSPARQL11UpdateComplianceTest().getTestData(); }
	 * @TestFactory public Collection<DynamicTest> quadQuery() { return new
	 * EndpointQuadSPARQL11QueryComplianceTest().tests(); }
	 * @TestFactory public Collection<DynamicTest> quadUpdate() { return new
	 * EndpointQuadSPARQL11UpdateComplianceTest().getTestData(); }
	 */
}
