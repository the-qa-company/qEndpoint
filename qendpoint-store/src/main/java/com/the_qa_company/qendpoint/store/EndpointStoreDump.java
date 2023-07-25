package com.the_qa_company.qendpoint.store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * information on how a {@link EndpointStore} should be dumped
 *
 * @author Antoine Willerval
 */
public interface EndpointStoreDump {

	static EndpointStoreDump dataset(Path outputLocation) {
		return new EndpointStoreDumpDataset(outputLocation);
	}

	/**
	 * call before the dataset catdiff
	 *
	 * @param store store
	 * @throws IOException io exception
	 */
	default void beforeMerge(EndpointStore store) throws IOException {
	}

	/**
	 * call after the dataset catdiff
	 *
	 * @param store         store
	 * @param mergedDataset path were the dataset was catdiff
	 * @throws IOException io exception
	 */
	default void afterMerge(EndpointStore store, Path mergedDataset) throws IOException {
	}

	class EndpointStoreDumpDataset implements EndpointStoreDump {
		protected final Path outputLocation;

		public EndpointStoreDumpDataset(Path outputLocation) {
			this.outputLocation = outputLocation;
		}

		@Override
		public void afterMerge(EndpointStore store, Path mergedDataset) throws IOException {
			Files.createDirectories(outputLocation.getParent());
			// store the dataset
			Files.copy(mergedDataset, outputLocation.resolve("store.hdt"));
		}
	}
}
