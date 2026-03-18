package com.the_qa_company.qendpoint.core.util.io;

/**
 * Toggle LZ4 compression across the codebase.
 * <p>
 * Set to {@code false} to disable LZ4 compression and use raw data instead.
 */
public final class Lz4Config {
	public static volatile boolean ENABLED = true;

	private Lz4Config() {
	}

	public static void setEnabled(boolean enabled) {
		ENABLED = enabled;
	}

	public static boolean isEnabled() {
		return ENABLED;
	}
}
