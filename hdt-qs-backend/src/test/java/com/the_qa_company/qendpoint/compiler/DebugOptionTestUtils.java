package com.the_qa_company.qendpoint.compiler;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;

public class DebugOptionTestUtils {
	public static Runnable setOptimization(boolean value) {
		final boolean opt = CompiledSailOptions.defaultOptimization;
		CompiledSailOptions.defaultOptimization = value;
		return () -> CompiledSailOptions.defaultOptimization = opt;
	}

	public static Runnable setStorageMode(IRI storage) {
		final IRI dv = CompiledSailOptions.defaultStorageMode;
		CompiledSailOptions.defaultStorageMode = storage;
		return () -> CompiledSailOptions.defaultStorageMode = dv;
	}

	public static void setPassMode(CompiledSailOptions opt, Value mode) {
		opt.storageMode = SailCompilerSchema.HDT_PASS_MODE_PROPERTY.throwIfNotValidValue(mode);
	}
}
