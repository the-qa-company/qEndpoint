package com.the_qa_company.qendpoint.core.util.crc;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class CRCStreamTest {
	public static final int size = 1000000;

	@Parameterized.Parameters(name = "CRC{0}")
	public static List<Object> params() {
		return List.of(8, 16, 32);
	}

	@Parameterized.Parameter
	public int len;

	public CRC crc() {
		return switch (len) {
		case 8 -> new CRC8();
		case 16 -> new CRC16();
		case 32 -> new CRC32();
		default -> throw new AssertionError("Invalid CRC" + len);
		};
	}

	@Test
	public void testStreamCRC() throws Exception {
		ByteArrayOutputStream byteStrOut = new ByteArrayOutputStream(size + len / 8);

		CRCOutputStream crcStrmOut = new CRCOutputStream(byteStrOut, crc());
		for (int i = 0; i < size; i++) {
			crcStrmOut.write(i & 0xFF);
		}
		crcStrmOut.writeCRC();
		crcStrmOut.close();
//		System.out.println("CRC: "+crcStrmOut.crc);

		ByteArrayInputStream byteStrIn = new ByteArrayInputStream(byteStrOut.toByteArray());
		CRCInputStream crcStrmIn = new CRCInputStream(byteStrIn, crc());
		for (int i = 0; i < size; i++) {
			crcStrmIn.read();
		}
		assertTrue(crcStrmIn.readCRCAndCheck());
	}

	@Test
	public void testBufferCRC() throws Exception {
		ByteArrayOutputStream byteStrOut = new ByteArrayOutputStream(size + len / 8);

		CRCOutputStream crcStrmOut = new CRCOutputStream(byteStrOut, crc());
		for (int i = 0; i < size; i++) {
			crcStrmOut.write(i & 0xFF);
		}
		crcStrmOut.writeCRC();
		crcStrmOut.close();
//		System.out.println("CRC: "+crcStrmOut.crc);

		ByteArrayInputStream byteStrIn = new ByteArrayInputStream(byteStrOut.toByteArray());
		CRC crc = crc();
		crc.update(byteStrIn, size);
		assertTrue(crc.readAndCheck(byteStrIn));
	}

}
