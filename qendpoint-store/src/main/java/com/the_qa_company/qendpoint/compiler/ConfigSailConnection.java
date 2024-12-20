package com.the_qa_company.qendpoint.compiler;

import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;

/**
 * Interface to implement into a {@link org.eclipse.rdf4j.sail.SailConnection}
 * returned by the source of a {@link CompiledSail}, the configs will be added
 * using comments
 *
 * @author Antoine Willerval
 */
public interface ConfigSailConnection {
	ConfigSailConnection EMPTY = new ConfigSailConnection() {

		@Override
		public void setConfig(String cfg) {
			// ignore
		}

		@Override
		public void setConfig(String cfg, String value) {
			// ignore
		}

		@Override
		public boolean hasConfig(String cfg) {
			return false;
		}

		@Override
		public String getConfig(String cfg) {
			return null;
		}

		@Override
		public boolean allowUpdate() {
			return false;
		}
	};

	/**
	 * set a config without any value
	 *
	 * @param cfg the config
	 */
	void setConfig(String cfg);

	/**
	 * set a config with a value
	 *
	 * @param cfg   the config
	 * @param value the config value
	 */
	void setConfig(String cfg, String value);

	/**
	 * test if the connection has a config
	 *
	 * @param cfg the config
	 * @return true if the config was set
	 */
	boolean hasConfig(String cfg);

	/**
	 * get a config value
	 *
	 * @param cfg the config
	 * @return the config value or null if unset
	 */
	String getConfig(String cfg);

	/**
	 * @return if the connection allows to set config
	 */
	boolean allowUpdate();
}
