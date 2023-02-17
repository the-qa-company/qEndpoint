package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.triples.IndexedNode;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.CharSequenceComparator;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class CompressTest {
	public static void assertCharSequenceEquals(String location, CharSequence s1, CharSequence s2) {
		if (CharSequenceComparator.getInstance().compare(s1, s2) != 0) {
			throw new AssertionError(location + "\nexcepted: " + s1 + "\nactual:   " + s2);
		}
	}

	@Test
	public void noDupeTest() {
		List<IndexedNode> duplicatedList = Arrays.asList(new IndexedNode("a", 0), new IndexedNode("b", 1),
				new IndexedNode("b", 2), new IndexedNode("c", 3), new IndexedNode("c", 4), new IndexedNode("c", 5),
				new IndexedNode("d", 6), new IndexedNode("e", 7), new IndexedNode("f", 8));
		List<CharSequence> noDuplicatedList = Arrays.asList("a", "b", "c", "d", "e", "f");

		Set<Long> duplicates = new HashSet<>();
		duplicates.add(2L);
		duplicates.add(4L);
		duplicates.add(5L);

		Iterator<IndexedNode> actual = CompressUtil
				.asNoDupeCharSequenceIterator(ExceptionIterator.of(duplicatedList.iterator()).map(in -> {
					in.setNode(ByteString.of(in.getNode()));
					return in;
				}), (originalIndex, duplicatedIndex, oldIndex) -> Assert
						.assertTrue(duplicates.remove(duplicatedIndex)));
		for (CharSequence e : noDuplicatedList) {
			Assert.assertTrue(actual.hasNext());
			CharSequence a = actual.next().getNode();

			assertCharSequenceEquals("noDupeTest", e, a);
		}
	}

	@Test
	public void bitMappingTest() {
		long sharedCount = 1000L;
		long index1 = 888L;

		long sharedIndex1 = CompressUtil.asShared(index1);

		Assert.assertEquals(index1, CompressUtil.computeSharedNode(sharedIndex1, sharedCount));
		Assert.assertEquals(sharedCount + index1,
				CompressUtil.computeSharedNode(CompressUtil.getHeaderId(index1), sharedCount));
	}

	@Test
	public void decodeSignedTest() throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();

		VByte.encodeSigned(out, 0xFFL);
		VByte.encodeSigned(out, -0xFFL);

		VByte.encodeSigned(out, 0x18L);
		VByte.encodeSigned(out, 0x91L);
		VByte.encodeSigned(out, 0x75L);

		VByte.encodeSigned(out, 0x186878L);
		VByte.encodeSigned(out, 0x9167L);
		VByte.encodeSigned(out, 0x75L);
		VByte.encodeSigned(out, -0x186878L);

		ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());

		Assert.assertEquals(0xFFL, VByte.decodeSigned(in));
		Assert.assertEquals(-0xFFL, VByte.decodeSigned(in));

		Assert.assertEquals(0x18L, VByte.decodeSigned(in));
		Assert.assertEquals(0x91L, VByte.decodeSigned(in));
		Assert.assertEquals(0x75L, VByte.decodeSigned(in));

		Assert.assertEquals(0x186878L, VByte.decodeSigned(in));
		Assert.assertEquals(0x9167L, VByte.decodeSigned(in));
		Assert.assertEquals(0x75L, VByte.decodeSigned(in));
		Assert.assertEquals(-0x186878L, VByte.decodeSigned(in));
	}
}
