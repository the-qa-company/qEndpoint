package com.the_qa_company.qendpoint.core.util.crc;

import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * CRC16-ANSI Width = 16 Poly = 0x8005 XorIn = 0x0000 ReflectIn = True XorOut =
 * 0x0000 ReflectOut = True
 *
 * @author mario.arias
 */
public class CRC16 implements CRC {
	int crc16;

	private static final short[] crc16_table = { (short) 0x0000, (short) 0xc0c1, (short) 0xc181, (short) 0x0140,
			(short) 0xc301, (short) 0x03c0, (short) 0x0280, (short) 0xc241, (short) 0xc601, (short) 0x06c0,
			(short) 0x0780, (short) 0xc741, (short) 0x0500, (short) 0xc5c1, (short) 0xc481, (short) 0x0440,
			(short) 0xcc01, (short) 0x0cc0, (short) 0x0d80, (short) 0xcd41, (short) 0x0f00, (short) 0xcfc1,
			(short) 0xce81, (short) 0x0e40, (short) 0x0a00, (short) 0xcac1, (short) 0xcb81, (short) 0x0b40,
			(short) 0xc901, (short) 0x09c0, (short) 0x0880, (short) 0xc841, (short) 0xd801, (short) 0x18c0,
			(short) 0x1980, (short) 0xd941, (short) 0x1b00, (short) 0xdbc1, (short) 0xda81, (short) 0x1a40,
			(short) 0x1e00, (short) 0xdec1, (short) 0xdf81, (short) 0x1f40, (short) 0xdd01, (short) 0x1dc0,
			(short) 0x1c80, (short) 0xdc41, (short) 0x1400, (short) 0xd4c1, (short) 0xd581, (short) 0x1540,
			(short) 0xd701, (short) 0x17c0, (short) 0x1680, (short) 0xd641, (short) 0xd201, (short) 0x12c0,
			(short) 0x1380, (short) 0xd341, (short) 0x1100, (short) 0xd1c1, (short) 0xd081, (short) 0x1040,
			(short) 0xf001, (short) 0x30c0, (short) 0x3180, (short) 0xf141, (short) 0x3300, (short) 0xf3c1,
			(short) 0xf281, (short) 0x3240, (short) 0x3600, (short) 0xf6c1, (short) 0xf781, (short) 0x3740,
			(short) 0xf501, (short) 0x35c0, (short) 0x3480, (short) 0xf441, (short) 0x3c00, (short) 0xfcc1,
			(short) 0xfd81, (short) 0x3d40, (short) 0xff01, (short) 0x3fc0, (short) 0x3e80, (short) 0xfe41,
			(short) 0xfa01, (short) 0x3ac0, (short) 0x3b80, (short) 0xfb41, (short) 0x3900, (short) 0xf9c1,
			(short) 0xf881, (short) 0x3840, (short) 0x2800, (short) 0xe8c1, (short) 0xe981, (short) 0x2940,
			(short) 0xeb01, (short) 0x2bc0, (short) 0x2a80, (short) 0xea41, (short) 0xee01, (short) 0x2ec0,
			(short) 0x2f80, (short) 0xef41, (short) 0x2d00, (short) 0xedc1, (short) 0xec81, (short) 0x2c40,
			(short) 0xe401, (short) 0x24c0, (short) 0x2580, (short) 0xe541, (short) 0x2700, (short) 0xe7c1,
			(short) 0xe681, (short) 0x2640, (short) 0x2200, (short) 0xe2c1, (short) 0xe381, (short) 0x2340,
			(short) 0xe101, (short) 0x21c0, (short) 0x2080, (short) 0xe041, (short) 0xa001, (short) 0x60c0,
			(short) 0x6180, (short) 0xa141, (short) 0x6300, (short) 0xa3c1, (short) 0xa281, (short) 0x6240,
			(short) 0x6600, (short) 0xa6c1, (short) 0xa781, (short) 0x6740, (short) 0xa501, (short) 0x65c0,
			(short) 0x6480, (short) 0xa441, (short) 0x6c00, (short) 0xacc1, (short) 0xad81, (short) 0x6d40,
			(short) 0xaf01, (short) 0x6fc0, (short) 0x6e80, (short) 0xae41, (short) 0xaa01, (short) 0x6ac0,
			(short) 0x6b80, (short) 0xab41, (short) 0x6900, (short) 0xa9c1, (short) 0xa881, (short) 0x6840,
			(short) 0x7800, (short) 0xb8c1, (short) 0xb981, (short) 0x7940, (short) 0xbb01, (short) 0x7bc0,
			(short) 0x7a80, (short) 0xba41, (short) 0xbe01, (short) 0x7ec0, (short) 0x7f80, (short) 0xbf41,
			(short) 0x7d00, (short) 0xbdc1, (short) 0xbc81, (short) 0x7c40, (short) 0xb401, (short) 0x74c0,
			(short) 0x7580, (short) 0xb541, (short) 0x7700, (short) 0xb7c1, (short) 0xb681, (short) 0x7640,
			(short) 0x7200, (short) 0xb2c1, (short) 0xb381, (short) 0x7340, (short) 0xb101, (short) 0x71c0,
			(short) 0x7080, (short) 0xb041, (short) 0x5000, (short) 0x90c1, (short) 0x9181, (short) 0x5140,
			(short) 0x9301, (short) 0x53c0, (short) 0x5280, (short) 0x9241, (short) 0x9601, (short) 0x56c0,
			(short) 0x5780, (short) 0x9741, (short) 0x5500, (short) 0x95c1, (short) 0x9481, (short) 0x5440,
			(short) 0x9c01, (short) 0x5cc0, (short) 0x5d80, (short) 0x9d41, (short) 0x5f00, (short) 0x9fc1,
			(short) 0x9e81, (short) 0x5e40, (short) 0x5a00, (short) 0x9ac1, (short) 0x9b81, (short) 0x5b40,
			(short) 0x9901, (short) 0x59c0, (short) 0x5880, (short) 0x9841, (short) 0x8801, (short) 0x48c0,
			(short) 0x4980, (short) 0x8941, (short) 0x4b00, (short) 0x8bc1, (short) 0x8a81, (short) 0x4a40,
			(short) 0x4e00, (short) 0x8ec1, (short) 0x8f81, (short) 0x4f40, (short) 0x8d01, (short) 0x4dc0,
			(short) 0x4c80, (short) 0x8c41, (short) 0x4400, (short) 0x84c1, (short) 0x8581, (short) 0x4540,
			(short) 0x8701, (short) 0x47c0, (short) 0x4680, (short) 0x8641, (short) 0x8201, (short) 0x42c0,
			(short) 0x4380, (short) 0x8341, (short) 0x4100, (short) 0x81c1, (short) 0x8081, (short) 0x4040 };

	@Override
	public void update(byte[] buffer, int offset, int length) {
		int len = length;
		int i = offset;

		while (len-- > 0) {
			int tbl_idx = (crc16 ^ buffer[i]) & 0xff;
			crc16 = (crc16_table[tbl_idx] ^ crc16 >>> 8) & 0xFFFF;
			i++;
		}
	}

	@Override
	public void update(byte data) {
		int tbl_idx = (crc16 ^ data) & 0xff;

		crc16 = (crc16_table[tbl_idx] ^ crc16 >>> 8) & 0xFFFF;
	}

	@Override
	public void writeCRC(OutputStream out) throws IOException {
		IOUtil.writeShort(out, (short) crc16);
	}

	@Override
	public boolean readAndCheck(InputStream in) throws IOException {
		int readCRC = IOUtil.readShort(in) & 0xFFFF;
		return readCRC == crc16;
	}

	@Override
	public long getValue() {
		return crc16;
	}

	@Override
	public void reset() {
		crc16 = 0;
	}

	@Override
	public int compareTo(CRC o) {
		if (o instanceof CRC16) {
			return ((CRC16) o).crc16 - this.crc16;
		}
		throw new RuntimeException("Cannot compare CRC's of different types");
	}

	@Override
	public String toString() {
		return Long.toHexString(getValue() & 0xFFFFL);
	}
}
