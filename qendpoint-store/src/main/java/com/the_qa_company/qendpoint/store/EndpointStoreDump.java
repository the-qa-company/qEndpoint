package com.the_qa_company.qendpoint.store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

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

	/**
	 * call after the indexing
	 *
	 * @param store              store
	 * @param mergedDatasetIndex v11 index
	 * @throws IOException any
	 * @deprecated use {@link #afterIndexing(EndpointStore, List)} instead
	 */
	@Deprecated
	default void afterIndexing(EndpointStore store, Path mergedDatasetIndex) throws IOException {
	}

	/**
	 * call after the indexing
	 *
	 * @param store                store
	 * @param mergedDatasetIndexes indexes
	 * @throws IOException any
	 */
	default void afterIndexing(EndpointStore store, List<Path> mergedDatasetIndexes) throws IOException {
		if (!mergedDatasetIndexes.isEmpty()) {
			afterIndexing(store, mergedDatasetIndexes.get(0));
		}
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

		static String replaceHDTFilename(Path path, String newName) {
			String filename = path.getFileName().toString();

			int idx = filename.lastIndexOf(".hdt");

			if (idx == -1)
				throw new IllegalArgumentException("Not a HDT file");

			return newName + filename.substring(idx);
		}

		@Override
		public void afterIndexing(EndpointStore store, List<Path> mergedDatasetIndexes) throws IOException {
			Files.createDirectories(outputLocation.getParent());
			// store the dataset
			for (Path path : mergedDatasetIndexes) {
				if (Files.exists(path)) {
					Files.copy(path, outputLocation.resolve(replaceHDTFilename(path, "store")));
				}
			}
		}
	}
}
