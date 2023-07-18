package com.the_qa_company.qendpoint.core.util.nsd;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;

public class NSDReader10 implements NSDBinaryReader {

	@Override
	public byte version() {
		return 0x10;
	}

	@Override
	public void readData(NamespaceData data, InputStream stream, ProgressListener ls) throws IOException {
		CRCInputStream is = new CRCInputStream(stream, new CRC32());
		long count = VByte.decode(is);

		if (count < 0) {
			throw new IOException(format("Negative count read: %d", count));
		}

		Map<String, String> tempRead = new HashMap<>();

		for (long i = 0; i < count; i++) {
			String key = IOUtil.readSizedString(is, ls);
			String value = IOUtil.readSizedString(is, ls);
			String old = tempRead.put(key, value);
			if (old != null) {
				throw new IOException(format("Read twice the same namespace key: %s", key));
			}
		}

		if (!is.readCRCAndCheck()) {
			throw new IOException("Invalid CRC!");
		}
		// the file was read correctly, we can replace the namespace
		data.namespaces.clear();
		data.namespaces.putAll(tempRead);
	}
}
