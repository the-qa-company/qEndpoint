package com.the_qa_company.qendpoint.compiler;

import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.NotifyingSail;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.NotifyingSailConnectionWrapper;
import org.eclipse.rdf4j.sail.helpers.NotifyingSailWrapper;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConfigSailConnectionTest {
	@Test
	public void test() throws IOException {
		TestSail testSail = new TestSail(new MemoryStore());
		SparqlRepository repo = CompiledSail.compiler().withSourceSail(testSail).compileToSparqlRepository();
		try {
			repo.init();

			try (SailRepositoryConnection rpc = repo.getConnection()) {
				if (!(rpc.getSailConnection() instanceof CompiledSail.CompiledSailConnection csc)) {
					fail("repo connection should be a CompiledSailConnection, but is "
							+ rpc.getSailConnection().getClass());
					return;
				}
				if (!(csc.getSourceConnection() instanceof ConfigSailConnection configConn)) {
					fail("source connection should be a ConfigSailConnection, but is "
							+ csc.getSourceConnection().getClass());
					return;
				}

				repo.executeTupleQuery(rpc, """
						#cfg_value:value_v
						#cfg_no_value
						SELECT * {?s ?p ?o}
						""", 0).close();

				assertTrue(configConn.hasConfig("cfg_no_value"));
				assertEquals(configConn.getConfig("cfg_value"), "value_v");
			}
		} finally {
			repo.shutDown();
		}
	}

	private static class TestSail extends NotifyingSailWrapper {
		public TestSail(NotifyingSail baseSail) {
			super(baseSail);
		}

		@Override
		public NotifyingSailConnection getConnection() throws SailException {
			return new TestSailConnection(super.getConnection());
		}
	}

	private static class TestSailConnection extends NotifyingSailConnectionWrapper implements ConfigSailConnection {

		private final Map<String, String> config = new HashMap<>();

		public TestSailConnection(NotifyingSailConnection baseSail) {
			super(baseSail);
		}

		@Override
		public void setConfig(String cfg) {
			config.put(cfg, "");
		}

		@Override
		public void setConfig(String cfg, String value) {
			config.put(cfg, value);
		}

		@Override
		public boolean hasConfig(String cfg) {
			return config.containsKey(cfg);
		}

		@Override
		public String getConfig(String cfg) {
			return config.get(cfg);
		}

		@Override
		public boolean allowUpdate() {
			return true;
		}
	}
}
