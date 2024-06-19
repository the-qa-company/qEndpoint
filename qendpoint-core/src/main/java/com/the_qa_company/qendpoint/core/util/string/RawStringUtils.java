package com.the_qa_company.qendpoint.core.util.string;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import com.the_qa_company.qendpoint.core.util.Mutable;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import org.apache.jena.vocabulary.XSD;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

public final class RawStringUtils {
	private RawStringUtils() {
	}

	public static final byte RAW_TYPE_RAW = '#';
	public static final byte RAW_TYPE_LIT_NDT = 'N';
	public static final byte RAW_TYPE_LIT_DT = '^';
	public static final byte RAW_TYPE_LIT_DT_INT = 'I';
	public static final byte RAW_TYPE_LIT_DT_FLOAT = 'F';
	public static final byte RAW_TYPE_LIT_DT_DEC = 'D';
	public static final byte RAW_TYPE_LIT_LG = '@';

	public static final ByteString XSD_START = ByteString.of("<" + XSD.NS);
	public static final ByteString XSD_DECIMAL_DT = XSD_START.copyAppend("decimal>");
	public static final ByteString XSD_DOUBLE_DT = XSD_START.copyAppend("double>");
	public static final ByteString XSD_INTEGER_DT = XSD_START.copyAppend("integer>");

	public static ByteString rawKnownDataType(ByteString rs) {
		if (!rs.startsWith(XSD_START)) {
			return rs;
		}

		if (rs.length() == XSD_DECIMAL_DT.length()
				&& rs.startsWith(XSD_DECIMAL_DT, XSD_START.length(), XSD_START.length())) {
			return XSD_DECIMAL_DT;
		}

		if (rs.length() == XSD_DOUBLE_DT.length()
				&& rs.startsWith(XSD_DOUBLE_DT, XSD_START.length(), XSD_START.length())) {
			return XSD_DOUBLE_DT;
		}

		if (rs.length() == XSD_INTEGER_DT.length()
				&& rs.startsWith(XSD_INTEGER_DT, XSD_START.length(), XSD_START.length())) {
			return XSD_INTEGER_DT;
		}

		return rs;
	}

	public static ByteString decimalLiteral(BigDecimal value) {
		return new TypedLiteralCompactString(ByteString.of(value), XSD_DECIMAL_DT);
	}

	public static ByteString integerLiteral(long value) {
		return new TypedLiteralCompactString(ByteString.of(value), XSD_INTEGER_DT);
	}

	public static ByteString floatLiteral(double value) {
		return new TypedLiteralCompactString(ByteString.of(value), XSD_DOUBLE_DT);
	}

	public static ByteString convertToRawString(CharSequence cs) {
		if (cs.isEmpty() || cs.charAt(0) != '"') {
			ByteString bs = ByteString.of(cs);
			// not a literal
			byte[] data = new byte[1 + bs.length()];

			data[0] = RAW_TYPE_RAW;

			System.arraycopy(bs.getBuffer(), 0, data, 1, data.length - 1);
			return new CompactString(data);
		}
		CharSequence type = LiteralsUtils.getType(cs);

		if (type == LiteralsUtils.LITERAL_LANG_TYPE) {
			Optional<CharSequence> lg = LiteralsUtils.getLanguage(cs);
			ByteString lang = ByteString.of(lg.orElseThrow());
			ByteString rem = ByteString.of(LiteralsUtils.removeLang(cs));

			byte[] data = new byte[1 + lang.length() + 1 + rem.length()];
			data[0] = RAW_TYPE_LIT_LG;
			System.arraycopy(lang.getBuffer(), 0, data, 1, lang.length());
			int idx = lang.length() + 1;
			data[idx] = 0;
			System.arraycopy(rem.getBuffer(), 0, data, idx + 1, rem.length());

			return new CompactString(data);
		}

		if (type == LiteralsUtils.NO_DATATYPE) {
			ByteString bs = ByteString.of(cs);
			// not a literal
			byte[] data = new byte[1 + bs.length()];

			data[0] = RAW_TYPE_LIT_NDT;

			System.arraycopy(bs.getBuffer(), 0, data, 1, data.length - 1);
			return new CompactString(data);
		}

		// type
		{
			ByteString rem = ByteString.of(LiteralsUtils.removeType(cs));
			ByteString tbs = ByteString.of(type);

			if (XSD_INTEGER_DT.equals(type)) {
				long val = Long.parseLong(rem, 1, rem.length() - 1, 10);

				int vblen = VByte.lenSigned(val);

				byte[] all = new byte[vblen + 1];
				all[0] = RAW_TYPE_LIT_DT_INT;
				VByte.encodeSigned(all, 1, val);

				return new CompactString(all);
			}
			if (XSD_DOUBLE_DT.equals(type)) {
				String valStr = rem.toString();
				double val = Double.parseDouble(valStr.substring(1, valStr.length() - 1));
				byte[] all = new byte[9];
				all[0] = RAW_TYPE_LIT_DT_FLOAT;
				IOUtil.writeLong(all, 1, Double.doubleToLongBits(val));

				return new CompactString(all);
			}

			if (XSD_DECIMAL_DT.equals(type)) {
				String valStr = rem.toString();
				BigDecimal dec = new BigDecimal(valStr.substring(1, valStr.length() - 1));
				int scale = dec.scale();
				byte[] ba = dec.unscaledValue().toByteArray();

				byte[] all = new byte[1 + VByte.lenSigned(scale) + VByte.len(ba.length) + ba.length];
				all[0] = RAW_TYPE_LIT_DT_DEC;
				int idx = 1 + VByte.encodeSigned(all, 1, scale);
				idx += VByte.encode(all, idx, ba.length);
				System.arraycopy(ba, 0, all, idx, ba.length);
				return new CompactString(all);
			}

			byte[] data = new byte[1 + tbs.length() + 1 + rem.length()];
			data[0] = RAW_TYPE_LIT_DT;
			System.arraycopy(tbs.getBuffer(), 0, data, 1, tbs.length());
			int idx = tbs.length() + 1;
			data[idx] = 0;
			System.arraycopy(rem.getBuffer(), 0, data, idx + 1, rem.length());

			return new CompactString(data);
		}
	}

	private static int findSplit(byte[] buffer, int length) {
		for (int i = 0; i < length; i++) {
			if (buffer[i] == '\0') {
				return i;
			}
		}
		return -1;
	}

	public static ByteString convertFromRawStringSec(CharSequence cs) {
		ByteString bs = ByteString.of(cs);
		byte[] buffer = bs.getBuffer();
		int length = bs.length();

		if (length == 0) {
			throw new IllegalArgumentException("Empty raw string");
		}

		switch (buffer[0]) {
			case RAW_TYPE_LIT_DT -> {
				int split = findSplit(buffer, length);
				if (split == -1) {
					throw new IllegalArgumentException("No split for datatype literal");
				}

				byte[] data = new byte[length - 2 + 2];
				System.arraycopy(buffer, split + 1, data, 0, length - split - 1);
				data[length - split - 1] = '^';
				data[length - split] = '^';
				System.arraycopy(buffer, 1, data, length - split + 1, split - 1);

				return new CompactString(data);
			}
			case RAW_TYPE_LIT_LG -> {
				int split = findSplit(buffer, length);
				if (split == -1) {
					throw new IllegalArgumentException("No split for lang literal");
				}
				byte[] data = new byte[length - 2 + 1];
				System.arraycopy(buffer, split + 1, data, 0, length - split - 1);
				data[length - split - 1] = '@';
				System.arraycopy(buffer, 1, data, length - split, split - 1);

				return new CompactString(data);
			}
			// no datatype
			case RAW_TYPE_LIT_NDT, RAW_TYPE_RAW -> { // raw type
				return bs.subSequence(1, length);
			}
			case RAW_TYPE_LIT_DT_FLOAT -> {
				if (length != 9) {
					throw new IllegalArgumentException("Invalid float type, 9 bytes required " + length);
				}

				return new DoubleCompactString(Double.longBitsToDouble(IOUtil.readLong(buffer, 1)));
			}
			case RAW_TYPE_LIT_DT_DEC -> {
				Mutable<Long> value = new Mutable<>(0L);

				int idx = 1 + VByte.decodeSigned(buffer, 1, value);

				int scale = value.getValue().intValue();

				idx += VByte.decode(buffer, idx, value);

				int buffSize = value.getValue().intValue();

				BigInteger unscale = new BigInteger(buffer, idx, buffSize);

				return new DecimalCompactString(new BigDecimal(unscale, scale));
			}
			case RAW_TYPE_LIT_DT_INT -> {
				Mutable<Long> value = new Mutable<>(0L);

				VByte.decodeSigned(buffer, 1, value);

				return new IntCompactString(value.getValue());
			}
			default -> throw new IllegalArgumentException("Invalid raw string type " + (char) buffer[0]);
		}
	}

	public static ByteString convertFromRawString(CharSequence cs) {
		ByteString bs = ByteString.of(cs);
		byte[] buffer = bs.getBuffer();
		int length = bs.length();

		if (length == 0) {
			throw new IllegalArgumentException("Empty raw string");
		}

		switch (buffer[0]) {
		case RAW_TYPE_LIT_DT -> {
			int split = findSplit(buffer, length);
			if (split == -1) {
				throw new IllegalArgumentException("No split for datatype literal");
			}

			byte[] data = new byte[length - 2 + 2];
			System.arraycopy(buffer, split + 1, data, 0, length - split - 1);
			data[length - split - 1] = '^';
			data[length - split] = '^';
			System.arraycopy(buffer, 1, data, length - split + 1, split - 1);

			return new CompactString(data);
		}
		case RAW_TYPE_LIT_LG -> {
			int split = findSplit(buffer, length);
			if (split == -1) {
				throw new IllegalArgumentException("No split for lang literal");
			}
			byte[] data = new byte[length - 2 + 1];
			System.arraycopy(buffer, split + 1, data, 0, length - split - 1);
			data[length - split - 1] = '@';
			System.arraycopy(buffer, 1, data, length - split, split - 1);

			return new CompactString(data);
		}
		// no datatype
		case RAW_TYPE_LIT_NDT, RAW_TYPE_RAW -> { // raw type
			return bs.subSequence(1, length);
		}
		case RAW_TYPE_LIT_DT_FLOAT -> {
			if (length != 9) {
				throw new IllegalArgumentException("Invalid float type, 9 bytes required " + length);
			}

			return floatLiteral(Double.longBitsToDouble(IOUtil.readLong(buffer, 1)));
		}
		case RAW_TYPE_LIT_DT_DEC -> {
			Mutable<Long> value = new Mutable<>(0L);

			int idx = 1 + VByte.decodeSigned(buffer, 1, value);

			int scale = value.getValue().intValue();

			idx += VByte.decode(buffer, idx, value);

			int buffSize = value.getValue().intValue();

			BigInteger unscale = new BigInteger(buffer, idx, buffSize);

			return decimalLiteral(new BigDecimal(unscale, scale));
		}
		case RAW_TYPE_LIT_DT_INT -> {
			Mutable<Long> value = new Mutable<>(0L);

			VByte.decodeSigned(buffer, 1, value);

			return integerLiteral(value.getValue());
		}
		default -> throw new IllegalArgumentException("Invalid raw string type " + (char) buffer[0]);
		}
	}

	public static int compareRawString(CharSequence s1, CharSequence s2) {
		ByteString b1 = ByteString.of(s1);
		ByteString b2 = ByteString.of(s2);

		char t1 = b1.charAt(0);
		char t2 = b2.charAt(0);

		int c = Character.compare(t1, t2);

		if (c != 0) {
			return c; // useless to compare
		}

		switch (t1) {
		case RAW_TYPE_RAW, RAW_TYPE_LIT_NDT, RAW_TYPE_LIT_DT, RAW_TYPE_LIT_LG -> {
			return b1.compareTo(b2); // maybe ignore first?
		}
		case RAW_TYPE_LIT_DT_INT -> {
			byte[] bb1 = b1.getBuffer();
			byte[] bb2 = b2.getBuffer();

			Mutable<Long> value = new Mutable<>(0L);

			VByte.decodeSigned(bb1, 1, value);
			long v1 = value.getValue();
			VByte.decodeSigned(bb2, 1, value);
			long v2 = value.getValue();
			return Long.compare(v1, v2);
		}
		case RAW_TYPE_LIT_DT_FLOAT -> {
			byte[] bb1 = b1.getBuffer();
			int bl1 = b1.length();
			byte[] bb2 = b2.getBuffer();
			int bl2 = b2.length();

			if (bl1 != 9) {
				throw new IllegalArgumentException("Invalid float len " + bl1);
			}
			if (bl2 != 9) {
				throw new IllegalArgumentException("Invalid float len " + bl2);
			}

			double d1 = Double.longBitsToDouble(IOUtil.readLong(bb1, 1));
			double d2 = Double.longBitsToDouble(IOUtil.readLong(bb2, 1));
			return Double.compare(d1, d2);
		}
		case RAW_TYPE_LIT_DT_DEC -> {
			byte[] bb1 = b1.getBuffer();
			byte[] bb2 = b2.getBuffer();

			Mutable<Long> value = new Mutable<>(0L);
			BigDecimal bd1, bd2;
			{
				int idx = 1 + VByte.decodeSigned(bb1, 1, value);

				int scale = value.getValue().intValue();

				idx += VByte.decode(bb1, idx, value);

				int buffSize = value.getValue().intValue();

				BigInteger unscale = new BigInteger(bb1, idx, buffSize);
				bd1 = new BigDecimal(unscale, scale);
			}
			{
				int idx = 1 + VByte.decodeSigned(bb2, 1, value);

				int scale = value.getValue().intValue();

				idx += VByte.decode(bb2, idx, value);

				int buffSize = value.getValue().intValue();

				BigInteger unscale = new BigInteger(bb2, idx, buffSize);
				bd2 = new BigDecimal(unscale, scale);
			}

			return bd1.compareTo(bd2);
		}
		default -> throw new IllegalArgumentException("Invalid raw string type: " + t1);
		}
	}

	public static ByteString rawType(ByteString rs) {
		char b0 = rs.charAt(0);
		switch (b0) {
			case RAW_TYPE_LIT_DT -> {
				byte[] b = rs.getBuffer();
				int length = rs.length();
				int split = findSplit(b, length);

				if (split == -1) {
					throw new IllegalArgumentException("Invalid datatype literal: " + rs);
				}
				return rs.subSequence(1, split);
			}
			case RAW_TYPE_RAW, RAW_TYPE_LIT_NDT -> {
				return LiteralsUtils.NO_DATATYPE;
			}
			case RAW_TYPE_LIT_DT_INT -> {
				return XSD_INTEGER_DT;
			}
			case RAW_TYPE_LIT_DT_FLOAT -> {
				return XSD_DOUBLE_DT;
			}
			case RAW_TYPE_LIT_DT_DEC -> {
				return XSD_DECIMAL_DT;
			}
			case RAW_TYPE_LIT_LG -> {
				return LiteralsUtils.LITERAL_LANG_TYPE;
			}
			default -> throw new IllegalArgumentException("Invalid raw string type: " + b0);
		}
	}
}
