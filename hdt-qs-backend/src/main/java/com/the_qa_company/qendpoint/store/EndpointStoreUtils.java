package com.the_qa_company.qendpoint.store;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class to debug the endpoint store
 *
 * @author Antoine Willerval
 */
public class EndpointStoreUtils {
	private static final Map<Long, Map<Long, Throwable>> EP_TO_CO_TO_THR = new HashMap<>();
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

	private EndpointStoreUtils() {
	}
}
