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

	// debug only
	static CompiledSailOptions debugOptions;

	/**
	 * disable the loading of the config
	 */
	private boolean debugDisableLoading;
	private boolean debugShowTime;
	private boolean debugShowPlans;
	private boolean debugShowCount;
	private boolean optimization;
	private IRI storageMode;
	private IRI hdtReadMode;
	private IRI passMode;
	private int rdf4jSplitUpdate;
	private int endpointThreshold;
	private int port;
	private String hdtSpec;

	public CompiledSailOptions() {
		// set debug default values
		if (debugOptions != null) {
			debugDisableLoading = debugOptions.debugDisableLoading;
			debugShowTime = debugOptions.debugShowTime;
			debugShowPlans = debugOptions.debugShowPlans;
			optimization = debugOptions.optimization;
			debugShowCount = debugOptions.debugShowCount;
			storageMode = debugOptions.storageMode;
			hdtReadMode = debugOptions.hdtReadMode;
			passMode = debugOptions.passMode;
			rdf4jSplitUpdate = debugOptions.rdf4jSplitUpdate;
			endpointThreshold = debugOptions.endpointThreshold;
			port = debugOptions.port;
			hdtSpec = debugOptions.hdtSpec;
			return;
		}
		// set default values
		debugDisableLoading = false;
		debugShowTime = false;
		debugShowPlans = false;
		optimization = true;
		debugShowCount = false;
		storageMode = SailCompilerSchema.ENDPOINTSTORE_STORAGE;
		hdtReadMode = SailCompilerSchema.HDT_READ_MODE_MAP;
		passMode = SailCompilerSchema.HDT_TWO_PASS_MODE;
		rdf4jSplitUpdate = SailCompilerSchema.RDF_STORE_SPLIT_STORAGE.getHandler().defaultValue();
		endpointThreshold = SailCompilerSchema.ENDPOINT_THRESHOLD.getHandler().defaultValue();
		port = SailCompilerSchema.SERVER_PORT.getHandler().defaultValue();
		hdtSpec = "";
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
		port = reader.searchPropertyValue(SailCompilerSchema.MAIN, SailCompilerSchema.SERVER_PORT);
		hdtSpec = reader.searchPropertyValue(SailCompilerSchema.MAIN, SailCompilerSchema.HDT_SPEC_PROPERTY);
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

	public void setDebugDisableLoading(boolean debugDisableLoading) {
		this.debugDisableLoading = debugDisableLoading;
	}

	public boolean isDebugShowTime() {
		return debugShowTime;
	}

	public void setDebugShowTime(boolean debugShowTime) {
		this.debugShowTime = debugShowTime;
	}

	public boolean isDebugShowPlans() {
		return debugShowPlans;
	}

	public void setDebugShowPlans(boolean debugShowPlans) {
		this.debugShowPlans = debugShowPlans;
	}

	public boolean isDebugShowCount() {
		return debugShowCount;
	}

	public void setDebugShowCount(boolean debugShowCount) {
		this.debugShowCount = debugShowCount;
	}

	public boolean isOptimization() {
		return optimization;
	}

	public void setOptimization(boolean optimization) {
		this.optimization = optimization;
	}

	public IRI getStorageMode() {
		return storageMode;
	}

	public void setStorageMode(IRI storageMode) {
		this.storageMode = storageMode;
	}

	public IRI getHdtReadMode() {
		return hdtReadMode;
	}

	public void setHdtReadMode(IRI hdtReadMode) {
		this.hdtReadMode = hdtReadMode;
	}

	public IRI getPassMode() {
		return passMode;
	}

	public void setPassMode(IRI passMode) {
		this.passMode = passMode;
	}

	public int getRdf4jSplitUpdate() {
		return rdf4jSplitUpdate;
	}

	public void setRdf4jSplitUpdate(int rdf4jSplitUpdate) {
		this.rdf4jSplitUpdate = rdf4jSplitUpdate;
	}

	public int getEndpointThreshold() {
		return endpointThreshold;
	}

	public void setEndpointThreshold(int endpointThreshold) {
		this.endpointThreshold = endpointThreshold;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getHdtSpec() {
		return hdtSpec;
	}

	public void setHdtSpec(String hdtSpec) {
		this.hdtSpec = hdtSpec;
	}
}
