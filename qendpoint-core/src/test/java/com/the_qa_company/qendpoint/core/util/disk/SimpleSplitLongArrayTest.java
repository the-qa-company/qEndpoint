package com.the_qa_company.qendpoint.core.util.disk;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.IOException;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class SimpleSplitLongArrayTest {
	@Parameterized.Parameters(name = "numbits={0}")
	public static Collection<Object> params() {
		return IntStream.range(0, 6 + 1).mapToObj(i -> 1 << i).collect(Collectors.toList());
	}

	@Parameterized.Parameter
	public int bits;

	@Test
	public void setTest() throws IOException {
		LargeLongArray arrayExcepted = new LargeLongArray(IOUtil.createLargeArray(128L));
		try (SimpleSplitLongArray array = SimpleSplitLongArray.intXArray(128, bits)) {

			long max = (~0L) >>> (64 - bits);
			assertEquals("non matching size", arrayExcepted.length(), array.length());

			assertEquals("bad sizeof", bits, array.sizeOf());

			for (int i = 0; i < arrayExcepted.length(); i++) {
				arrayExcepted.set(i, i & max);
				array.set(i, i & max);
				assertEquals("non matching value #" + i, i & max, arrayExcepted.get(i));
				assertEquals("non matching value #" + i, i & max, array.get(i));
			}
			for (int i = 0; i < arrayExcepted.length(); i++) {
				assertEquals("non matching value #" + i, i & max, arrayExcepted.get(i));
				assertEquals("non matching value #" + i, i & max, array.get(i));
			}
		}
	}

	@Test
	public void resizeTest() throws IOException {
		LargeLongArray arrayExcepted = new LargeLongArray(IOUtil.createLargeArray(128L));
		try (SimpleSplitLongArray array = SimpleSplitLongArray.intXArray(128, bits)) {

			assertEquals("non matching size", arrayExcepted.length(), array.length());

			assertEquals("bad sizeof", bits, array.sizeOf());

			arrayExcepted.resize(256);
			array.resize(256);

			assertEquals("non matching size", arrayExcepted.length(), array.length());

			assertEquals("non matching size", arrayExcepted.length(), array.array.length() * (64 / bits));
		}
	}
}
