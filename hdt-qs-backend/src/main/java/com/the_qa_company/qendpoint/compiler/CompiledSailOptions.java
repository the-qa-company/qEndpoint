package com.the_qa_company.qendpoint.compiler;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;

/**
 * Options loaded from the
 * {@link com.the_qa_company.qendpoint.compiler.SailCompiler.SailCompilerReader}.
 *
 * @author Antoine Willerval
 */
public class CompiledSailOptions {
	static boolean defaultDebugDisableLoading;
	static boolean defaultDebugShowTime = false;
	static boolean defaultDebugShowPlans = false;
	static boolean defaultDebugShowCount = false;
	static boolean defaultOptimization = true;
	static IRI defaultStorageMode = SailCompilerSchema.ENDPOINTSTORE_STORAGE;
	static IRI defaultHdtReadMode = SailCompilerSchema.HDT_READ_MODE_MAP;
	static IRI defaultPassMode = SailCompilerSchema.HDT_TWO_PASS_MODE;
	static int defaultRdf4jSplitUpdate = SailCompilerSchema.RDF_STORE_SPLIT_STORAGE.getHandler().defaultValue();
	static int defaultEndpointThreshold = SailCompilerSchema.ENDPOINT_THRESHOLD.getHandler().defaultValue();

	public static void setDefaultEndpointThreshold(int defaultEndpointThreshold) {
		if (defaultEndpointThreshold < 0) {
			throw new IllegalArgumentException("Can't have a negative endpoint threshold!");
		}
		CompiledSailOptions.defaultEndpointThreshold = defaultEndpointThreshold;
	}

	/**
	 * disable the loading of the config
	 */
	boolean debugDisableLoading;

	boolean debugShowTime;
	boolean debugShowPlans;
	boolean debugShowCount;
	boolean optimization;
	IRI storageMode;
	IRI hdtReadMode;
	IRI passMode;
	int rdf4jSplitUpdate;
	int endpointThreshold;

	public CompiledSailOptions() {
		debugDisableLoading = defaultDebugDisableLoading;
		debugShowTime = defaultDebugShowTime;
		debugShowPlans = defaultDebugShowPlans;
		optimization = defaultOptimization;
		debugShowCount = defaultDebugShowCount;
		storageMode = defaultStorageMode;
		hdtReadMode = defaultHdtReadMode;
		passMode = defaultPassMode;
		rdf4jSplitUpdate = defaultRdf4jSplitUpdate;
		endpointThreshold = defaultEndpointThreshold;
	}

	/**
	 * read the options from a reader
	 *
	 * @param reader the reader
	 */
	void readOptions(SailCompiler.SailCompilerReader reader) {
		reader.search(SailCompilerSchema.MAIN, SailCompilerSchema.OPTION).forEach(this::add);
		storageMode = reader.searchPropertyValue(SailCompilerSchema.MAIN, SailCompilerSchema.STORAGE_MODE_PROPERTY);
		passMode = reader.searchPropertyValue(SailCompilerSchema.MAIN, SailCompilerSchema.HDT_PASS_MODE_PROPERTY);
		rdf4jSplitUpdate = reader.searchPropertyValue(SailCompilerSchema.MAIN,
				SailCompilerSchema.RDF_STORE_SPLIT_STORAGE);
		endpointThreshold = reader.searchPropertyValue(SailCompilerSchema.MAIN, SailCompilerSchema.ENDPOINT_THRESHOLD);
		hdtReadMode = reader.searchPropertyValue(SailCompilerSchema.MAIN, SailCompilerSchema.HDT_READ_MODE_PROPERTY);
	}

	private void add(Value value) {
		IRI iri = SailCompilerSchema.OPTION_PROPERTY.throwIfNotValidValue(value);

		if (SailCompilerSchema.DEBUG_SHOW_TIME.equals(iri)) {
			debugShowTime = true;
		} else if (SailCompilerSchema.DEBUG_SHOW_PLAN.equals(iri)) {
			debugShowPlans = true;
		} else if (SailCompilerSchema.NO_OPTIMIZATION.equals(iri)) {
			optimization = false;
		} else if (SailCompilerSchema.DEBUG_DISABLE_OPTION_RELOADING.equals(iri)) {
			debugDisableLoading = true;
		} else if (SailCompilerSchema.DEBUG_SHOW_QUERY_RESULT_COUNT.equals(iri)) {
			debugShowCount = true;
		} else {
			throw new SailCompiler.SailCompilerException("not implemented: " + iri);
		}
	}

	public boolean isDebugDisableLoading() {
		return debugDisableLoading;
	}

	public boolean isDebugShowTime() {
		return debugShowTime;
	}

	public boolean isDebugShowPlans() {
		return debugShowPlans;
	}

	public boolean isDebugShowCount() {
		return debugShowCount;
	}

	public boolean isOptimization() {
		return optimization;
	}

	public IRI getStorageMode() {
		return storageMode;
	}

	public IRI getHdtReadMode() {
		return hdtReadMode;
	}

	public IRI getPassMode() {
		return passMode;
	}

	public int getRdf4jSplitUpdate() {
		return rdf4jSplitUpdate;
	}

	public int getEndpointThreshold() {
		return endpointThreshold;
	}
}
