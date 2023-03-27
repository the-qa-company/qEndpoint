package com.the_qa_company.qendpoint.store;

import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.QueryParser;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.queryrender.QueryRenderer;
import org.eclipse.rdf4j.queryrender.sparql.experimental.SparqlQueryRenderer;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class to debug the endpoint store
 *
 * @author Antoine Willerval
 */
public class EndpointStoreUtils {
	private static final Map<Long, Map<Long, Throwable>> EP_TO_CO_TO_THR = new HashMap<>();
	private static final QueryRenderer QUERY_RENDERER = new SparqlQueryRenderer();
	private static final QueryParser QUERY_PARSER = QueryParserUtil.createParser(QueryLanguage.SPARQL);
	private static boolean debugConnection = false;

	/**
	 * enable the connection debug, non-closed connection will return an
	 * {@link java.lang.AssertionError} after shuting down an endpoint
	 */
	public static void enableDebugConnection() {
		debugConnection = true;
	}

	/**
	 * disable the connection debug
	 */
	public static void disableDebugConnection() {
		debugConnection = false;
	}

	static void openConnection(EndpointStoreConnection connection) {
		if (!debugConnection) {
			return;
		}
		long connectionId = EndpointStoreUtils.getDebugId(connection);
		long endpointId = EndpointStoreUtils.getDebugId(connection.getEndpoint());

		Map<Long, Throwable> coToThr = EP_TO_CO_TO_THR.get(endpointId);

		assert coToThr != null : "Open in a non-opened store";

		coToThr.put(connectionId, new Throwable("Co#" + connectionId + " / endpoint#" + endpointId));
	}

	static void closeConnection(EndpointStoreConnection connection) {
		if (!debugConnection) {
			return;
		}
		long connectionId = EndpointStoreUtils.getDebugId(connection);
		long endpointId = EndpointStoreUtils.getDebugId(connection.getEndpoint());

		Map<Long, Throwable> coToThr = EP_TO_CO_TO_THR.get(endpointId);

		assert coToThr != null : "Closing non-opened connection";

		coToThr.remove(connectionId);
	}

	static void openEndpoint(EndpointStore store) {
		if (!debugConnection) {
			return;
		}
		long endpointId = EndpointStoreUtils.getDebugId(store);

		EP_TO_CO_TO_THR.put(endpointId, new HashMap<>());
	}

	static void closeEndpoint(EndpointStore store) {
		if (!debugConnection) {
			return;
		}
		long endpointId = EndpointStoreUtils.getDebugId(store);

		Map<Long, Throwable> coToThr = EP_TO_CO_TO_THR.remove(endpointId);

		assert coToThr != null : "Closing non-opened endpoint";

		if (coToThr.isEmpty()) {
			return;
		}

		AssertionError ae = new AssertionError("endpoint closed with non closed connections");

		coToThr.values().forEach(ae::addSuppressed);

		throw ae;
	}

	/**
	 * get the debug id of a store
	 *
	 * @param store the store
	 * @return debug id
	 */
	public static long getDebugId(EndpointStore store) {
		return store.getDebugId();
	}

	/**
	 * get the debug id of a connection
	 *
	 * @param connection the connection
	 * @return debug id
	 */
	public static long getDebugId(EndpointStoreConnection connection) {
		return connection.getDebugId();
	}

	/**
	 * format a SPARQL query from string
	 *
	 * @param query query string
	 * @return formatter query
	 * @throws Exception formatting exception
	 */
	public static String formatSPARQLQuery(String query) throws Exception {
		return formatSPARQLQuery(query, null);
	}

	/**
	 * format a SPARQL query from string
	 *
	 * @param query   query string
	 * @param baseURI base URI
	 * @return formatter query
	 * @throws Exception formatting exception
	 */
	public static String formatSPARQLQuery(String query, String baseURI) throws Exception {
		return formatSPARQLQuery(QUERY_PARSER.parseQuery(query, baseURI));
	}

	/**
	 * format a SPARQL query from query
	 *
	 * @param query query
	 * @return formatter query
	 * @throws Exception formatting exception
	 */
	public static String formatSPARQLQuery(ParsedQuery query) throws Exception {
		return QUERY_RENDERER.render(query);
	}

	/**
	 * get a base URI from a file name
	 *
	 * @param rdfInput input file name
	 * @return base URI
	 */
	public static String baseURIFromFilename(String rdfInput) {
		String rdfInputLow = rdfInput.toLowerCase();
		if (rdfInputLow.startsWith("http") || rdfInputLow.startsWith("ftp")) {
			return URI.create(rdfInput).toString();
		} else {
			return Path.of(rdfInput).toUri().toString().replace(File.separatorChar, '/');
		}
	}

	private EndpointStoreUtils() {
	}
}
