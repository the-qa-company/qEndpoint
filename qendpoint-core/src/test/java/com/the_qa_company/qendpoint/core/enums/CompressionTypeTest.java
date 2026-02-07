package com.the_qa_company.qendpoint.core.enums;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.util.io.Lz4Config;
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

	@Test
	public void lz4UsesLz4JavaBlockStreams() throws Exception {
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		OutputStream out = CompressionType.LZ4.compress(os);
		try {
			assertEquals("Expected LZ4BlockOutputStream for LZ4 compression", "net.jpountz.lz4.LZ4BlockOutputStream",
					out.getClass().getName());
		} finally {
			out.close();
		}

		byte[] payload = os.toByteArray();
		InputStream in = CompressionType.LZ4.decompress(new ByteArrayInputStream(payload));
		try {
			assertEquals("Expected LZ4BlockInputStream for LZ4 decompression", "net.jpountz.lz4.LZ4BlockInputStream",
					in.getClass().getName());
		} finally {
			in.close();
		}
	}

	@Test
	public void lz4DisabledUsesIdentity() throws Exception {
		boolean original = Lz4Config.isEnabled();
		try {
			Lz4Config.setEnabled(false);
			byte[] payload = { 1, 2, 3, 4, 5 };
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			try (OutputStream out = CompressionType.LZ4.compress(os)) {
				out.write(payload);
			}
			assertArrayEquals(payload, os.toByteArray());
		} finally {
			Lz4Config.setEnabled(original);
		}
	}
}
