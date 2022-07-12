package com.the_qa_company.qendpoint.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Class to periodically fetch the prefixes from a json source
 *
 * @author Antoine Willerval
 */
public class PrefixFetcher extends Thread {
	/**
	 * the default prefix.cc url to fetch the prefixes
	 */
	public static final URL PREFIX_CC_URL;
	static {
		try {
			PREFIX_CC_URL = new URL("http://prefix.cc/popular/all.file.json");
		} catch (MalformedURLException e) {
			throw new Error(e);
		}
	}
	private static final Logger logger = LoggerFactory.getLogger(PrefixFetcher.class);

	private final ObjectMapper mapper = new ObjectMapper();
	private final PrefixOptions options;
	private Map<String, String> prefixes;
	private final Object PREFIXES_SYNC = new Object() {};

	/**
	 * create a fetcher
	 *
	 * @param options the options to config the fetcher
	 */
	public PrefixFetcher(PrefixOptions options) {
		super("PrefixFetcher");
		this.options = options;

		if (options.getStartMap() != null) {
			this.prefixes = options.getStartMap();
		} else if (!options.isWaitResponse()) {
			this.prefixes = new HashMap<>();
		}
	}

	/**
	 * @return the options for this fetcher
	 */
	public PrefixOptions getOptions() {
		return options;
	}

	/**
	 * get the prefixes, if waitResponse is set to true, will wait for the
	 * prefixes before returning the prefixes, otherwise, will return a empty
	 * map
	 *
	 * @return prefixes
	 * @throws InterruptedException if waitResponse is set to true and the
	 *                              thread is interrupted
	 */
	public Map<String, String> getPrefixes() throws InterruptedException {
		synchronized (PREFIXES_SYNC) {
			while (options.isWaitResponse() && prefixes == null) {
				PREFIXES_SYNC.wait();
			}
			return prefixes;
		}
	}

	private void waitNextTimeout() throws InterruptedException {
		Thread.sleep(options.getResetTimeout());
	}

	@Override
	public void run() {
		// variable to say if the last loop result was an error, to do not
		// render exception twice
		// for example if the user is running the endpoint offline
		boolean errorLastOne = false;

		while (!isInterrupted()) {
			try {
				// fetch the prefixes
				Map<String, String> prefixes = mapper.readValue(options.getFetcherURL(), new TypeReference<>() {});
				// set the prefixes
				synchronized (PREFIXES_SYNC) {
					this.prefixes = prefixes;
					PREFIXES_SYNC.notifyAll();
				}
				errorLastOne = false;
			} catch (IOException e) {
				if (!errorLastOne) {
					errorLastOne = true;
					logger.warn("Failed to fetch prefixes from {}", options.getFetcherURL(), e);
					synchronized (PREFIXES_SYNC) {
						if (prefixes == null) {
							PREFIXES_SYNC.notifyAll();
						}
					}
				}
			}
			try {
				waitNextTimeout();
			} catch (InterruptedException e) {
				return;
			}
		}
	}

	/**
	 * Options to config the prefix fetcher
	 *
	 * @author Antoine Willerval
	 */
	public static class PrefixOptions {
		private URL fetcherURL = PREFIX_CC_URL;
		private long resetTimeout = 1000L * 60 * 60;
		private boolean waitResponse = false;
		private Map<String, String> startMap;

		public URL getFetcherURL() {
			return fetcherURL;
		}

		/**
		 * set the fetcher URL, by default {@link #PREFIX_CC_URL}
		 *
		 * @param fetcherURL url
		 */
		public void setFetcherURL(URL fetcherURL) {
			this.fetcherURL = Objects.requireNonNull(fetcherURL, "fetcherURL can't be null!");
		}

		public long getResetTimeout() {
			return resetTimeout;
		}

		/**
		 * set the time between each fetch (in millis), by default 1h
		 * (3_600_000L)
		 *
		 * @param resetTimeout time (ms)
		 */
		public void setResetTimeout(long resetTimeout) {
			if (resetTimeout <= 0) {
				throw new IllegalArgumentException("resetTimeout can't be <= 0");
			}
			this.resetTimeout = resetTimeout;
		}

		public boolean isWaitResponse() {
			return waitResponse;
		}

		/**
		 * set if the {@link PrefixFetcher#getPrefixes()} method should wait for
		 * the fetch before the first result, by default false
		 *
		 * @param waitResponse true for waiting
		 */
		public void setWaitResponse(boolean waitResponse) {
			this.waitResponse = waitResponse;
		}

		public Map<String, String> getStartMap() {
			return startMap;
		}

		/**
		 * set the start map if {@link #getPrefixes()} is called before the
		 * first fetch, default null
		 *
		 * @param startMap start map, null to disable
		 */
		public void setStartMap(Map<String, String> startMap) {
			this.startMap = startMap;
		}
	}
}
