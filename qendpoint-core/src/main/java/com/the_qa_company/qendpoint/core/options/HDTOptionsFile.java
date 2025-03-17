package com.the_qa_company.qendpoint.core.options;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Set;

public class HDTOptionsFile {
	public static final long MAGIC = 0x4f464e4c44544448L;
	private HDTOptions options = HDTOptions.of();

	private final Path location;

	public HDTOptionsFile(Path location) {
		this.location = location;
	}

	public HDTOptions getOptions() {
		return options;
	}

	public void sync() throws IOException {
		HDTOptions options = HDTOptions.of();

		if (!Files.exists(location)) {
			this.options = options;
			return;
		}
		ProgressListener l = ProgressListener.ignore();
		try (CRCInputStream is = new CRCInputStream(new FastBufferedInputStream(Files.newInputStream(location)),
				new CRC32())) {
			if (IOUtil.readLong(is) != MAGIC)
				throw new IOException("Can't read HDTOptions file: Bad magic");

			long count = VByte.decode(is);
			for (long i = 0; i < count; i++) {
				String key = IOUtil.readSizedString(is, l);
				String val = IOUtil.readSizedString(is, l);
				if (!val.isEmpty()) {
					options.set(key, val);
				}
			}
			if (!is.readCRCAndCheck()) {
				throw new IOException("Can't read HDTOptions file: Bad CRC");
			}
		}
		this.options = options;
	}

	public void save() throws IOException {
		ProgressListener l = ProgressListener.ignore();
		try (CRCOutputStream os = new CRCOutputStream(new FastBufferedOutputStream(Files.newOutputStream(location)),
				new CRC32())) {
			IOUtil.writeLong(os, MAGIC);
			Set<?> keys = options.getKeys();
			VByte.encode(os, keys.size());
			for (Object k : keys) {
				String key = String.valueOf(k);
				String val = Objects.requireNonNull(options.get(key), "");
				IOUtil.writeSizedString(os, key, l);
				IOUtil.writeSizedString(os, val, l);
			}
			os.writeCRC();
		}
	}
}
