package com.the_qa_company.qendpoint.core.util.crc;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertTrue;

public class CRCStreamTest {

	@Before
	public void setUp() throws Exception {

	}

	public boolean testCRC(CRC generator, CRC checker, int size) throws Exception {
		ByteArrayOutputStream byteStrOut = new ByteArrayOutputStream(size + 10);

		CRCOutputStream crcStrmOut = new CRCOutputStream(byteStrOut, generator);
		for (int i = 0; i < size; i++) {
			crcStrmOut.write(i & 0xFF);
		}
		crcStrmOut.writeCRC();
		crcStrmOut.close();
//		System.out.println("CRC: "+crcStrmOut.crc);

		ByteArrayInputStream byteStrIn = new ByteArrayInputStream(byteStrOut.toByteArray());
		CRCInputStream crcStrmIn = new CRCInputStream(byteStrIn, checker);
		for (int i = 0; i < size; i++) {
			crcStrmIn.read();
		}
		boolean ok = crcStrmIn.readCRCAndCheck();
		crcStrmIn.close();
		return ok;
	}

	@Test
	public void testCRC8() throws Exception {
		assertTrue(testCRC(new CRC8(), new CRC8(), 1000 * 1000));
	}

	@Test
	public void testCRC16() throws Exception {
		assertTrue(testCRC(new CRC16(), new CRC16(), 1000 * 1000));
	}

	@Test
	public void testCRC32() throws Exception {
		assertTrue(testCRC(new CRC32(), new CRC32(), 1000 * 1000));
	}

}
