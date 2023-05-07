package com.the_qa_company.qendpoint.core.util.debug;

import com.the_qa_company.qendpoint.core.util.io.Closer;

import java.io.Closeable;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;

public class DebugWriter implements Closeable {
	private final Writer w;

	public DebugWriter(String id, boolean enabled) {
		if (enabled) {
			try {
				w = Files.newBufferedWriter(Path.of("debug-" + id + ".txt"));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		} else {
			w = null;
		}
	}

	public void debug(String format, Object... nodes) {
		if (w != null) {
			try {
				w.append(String.format(format, nodes)).append("\n");
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public boolean isEnabled() {
		return w != null;
	}

	@Override
	public void close() throws IOException {
		Closer.closeSingle(w);
	}
}
