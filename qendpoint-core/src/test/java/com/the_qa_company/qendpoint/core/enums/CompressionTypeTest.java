package com.the_qa_company.qendpoint.core.enums;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.junit.Assert.*;

public class CompressionTypeTest {

	@Test
	public void lz4Test() throws IOException {
		ByteArrayOutputStream os = new ByteArrayOutputStream();

		try (OutputStream los = CompressionType.LZ4.compress(os)) {
			VByte.encode(los, 12);
			VByte.encode(los, 123);
			VByte.encode(los, 1245);
		}

		try (InputStream is = CompressionType.LZ4.decompress(new ByteArrayInputStream(os.toByteArray()))) {
			assertEquals(12, VByte.decode(is));
			assertEquals(123, VByte.decode(is));
			assertEquals(1245, VByte.decode(is));
		}

	}
}
