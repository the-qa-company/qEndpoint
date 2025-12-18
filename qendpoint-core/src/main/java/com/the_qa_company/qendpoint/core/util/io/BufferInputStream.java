package com.the_qa_company.qendpoint.core.util.io;

import java.io.InputStream;

public abstract class BufferInputStream extends InputStream {
	public abstract boolean canRead(long len);

	public abstract long remaining();
}
