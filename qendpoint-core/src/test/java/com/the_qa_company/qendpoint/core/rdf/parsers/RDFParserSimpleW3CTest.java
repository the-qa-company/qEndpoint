package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDF;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

@RunWith(Parameterized.class)
public class RDFParserSimpleW3CTest {
	private static final String NTRIPLES_MANIFEST = "w3c/N-Triples/manifest.ttl";
	private static final String NQUADS_MANIFEST = "w3c/N-Quads/manifest.ttl";

	@Parameterized.Parameters(name = "{0}")
	public static Collection<TestCase> params() throws Exception {
		List<TestCase> tests = new ArrayList<>();
		tests.addAll(loadManifest(NTRIPLES_MANIFEST));
		tests.addAll(loadManifest(NQUADS_MANIFEST));
		tests.sort(Comparator.comparing((TestCase test) -> test.notation.name()).thenComparing(test -> test.name));
		return tests;
	}

	@Parameterized.Parameter
	public TestCase testCase;

	@Test
	public void runW3CTest() throws Exception {
		RDFParserSimple parser = new RDFParserSimple(true);
		try (InputStream input = testCase.action.openStream()) {
			parser.doParse(input, null, testCase.notation, true, (triple, pos) -> {});
			if (!testCase.expectSuccess) {
				Assert.fail("Expected parse failure for " + testCase.name + " (" + testCase.action + ")");
			}
		} catch (ParserException e) {
			if (testCase.expectSuccess) {
				throw e;
			}
		}
	}

	private static List<TestCase> loadManifest(String resourcePath) throws Exception {
		URL manifestUrl = requireResource(resourcePath);
		Model model = ModelFactory.createDefaultModel();
		try (InputStream input = manifestUrl.openStream()) {
			RDFDataMgr.read(model, input, manifestUrl.toString(), Lang.TURTLE);
		}

		Property action = model.createProperty("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#action");
		Property name = model.createProperty("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#name");

		Resource posNt = model.createResource("http://www.w3.org/ns/rdftest#TestNTriplesPositiveSyntax");
		Resource negNt = model.createResource("http://www.w3.org/ns/rdftest#TestNTriplesNegativeSyntax");
		Resource posNq = model.createResource("http://www.w3.org/ns/rdftest#TestNQuadsPositiveSyntax");
		Resource negNq = model.createResource("http://www.w3.org/ns/rdftest#TestNQuadsNegativeSyntax");

		List<TestCase> tests = new ArrayList<>();
		tests.addAll(collectTests(model, manifestUrl, posNt, RDFNotation.NTRIPLES, true, action, name));
		tests.addAll(collectTests(model, manifestUrl, negNt, RDFNotation.NTRIPLES, false, action, name));
		tests.addAll(collectTests(model, manifestUrl, posNq, RDFNotation.NQUAD, true, action, name));
		tests.addAll(collectTests(model, manifestUrl, negNq, RDFNotation.NQUAD, false, action, name));
		return tests;
	}

	private static List<TestCase> collectTests(Model model, URL manifestUrl, Resource testType, RDFNotation notation,
			boolean expectSuccess, Property action, Property name) {
		List<TestCase> tests = new ArrayList<>();
		ResIterator it = model.listResourcesWithProperty(RDF.type, testType);
		try {
			while (it.hasNext()) {
				Resource test = it.next();
				Statement actionStmt = test.getProperty(action);
				if (actionStmt == null) {
					continue;
				}
				RDFNode actionNode = actionStmt.getObject();
				String actionRef = actionNode.isResource() ? actionNode.asResource().getURI()
						: actionNode.asLiteral().getString();
				URL actionUrl = resolveUrl(manifestUrl, actionRef);
				String testName = resolveName(test, name);
				String display = notation.name() + ":" + testName + (expectSuccess ? "" : " [neg]");
				tests.add(new TestCase(display, testName, actionUrl, notation, expectSuccess));
			}
		} finally {
			it.close();
		}
		return tests;
	}

	private static String resolveName(Resource test, Property name) {
		Statement nameStmt = test.getProperty(name);
		if (nameStmt != null && nameStmt.getObject().isLiteral()) {
			String value = nameStmt.getString();
			if (value != null && !value.isEmpty()) {
				return value;
			}
		}
		return test.getLocalName();
	}

	private static URL requireResource(String resourcePath) {
		URL url = RDFParserSimpleW3CTest.class.getClassLoader().getResource(resourcePath);
		if (url == null) {
			throw new IllegalStateException("Missing resource: " + resourcePath);
		}
		return url;
	}

	private static URL resolveUrl(URL base, String ref) {
		try {
			return new URL(base, ref);
		} catch (MalformedURLException e) {
			throw new IllegalArgumentException("Invalid action URI: " + ref, e);
		}
	}

	public static class TestCase {
		private final String display;
		private final String name;
		private final URL action;
		private final RDFNotation notation;
		private final boolean expectSuccess;

		public TestCase(String display, String name, URL action, RDFNotation notation, boolean expectSuccess) {
			this.display = display;
			this.name = name;
			this.action = action;
			this.notation = notation;
			this.expectSuccess = expectSuccess;
		}

		@Override
		public String toString() {
			return display;
		}
	}
}
