package com.the_qa_company.qendpoint.core.dictionary.impl.section;

import com.the_qa_company.qendpoint.core.options.HDTSpecification;
import com.the_qa_company.qendpoint.core.triples.IndexedNode;
import org.junit.Assert;
import org.junit.Test;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.MapIterator;
import com.the_qa_company.qendpoint.core.util.io.compress.CompressUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class OneReadDictionarySectionTest {

	@Test
	public void sectionTest() throws IOException {
		List<IndexedNode> aa = Arrays.asList(new IndexedNode("1", 1), new IndexedNode("2", 2), new IndexedNode("2", 3),
				new IndexedNode("3", 4), new IndexedNode("4", 5), new IndexedNode("5", 6), new IndexedNode("5", 7),
				new IndexedNode("5", 8), new IndexedNode("6", 9), new IndexedNode("7", 10), new IndexedNode("8", 11),
				new IndexedNode("9", 12));

		try (OneReadDictionarySection sec1 = new OneReadDictionarySection(removeDupe(aa), aa.size())) {
			assertIteratorEquals(removeDupe(aa), sec1.getSortedEntries());
		}

		OneReadDictionarySection sec2 = new OneReadDictionarySection(removeDupe(aa), aa.size());

		try (PFCDictionarySection section = new PFCDictionarySection(new HDTSpecification())) {
			section.load(sec2, null);

			assertIteratorEquals(removeDupe(aa), section.getSortedEntries());
		}
	}

	private void assertIteratorEquals(Iterator<? extends CharSequence> it1, Iterator<? extends CharSequence> it2) {
		while (it1.hasNext()) {
			Assert.assertTrue(it2.hasNext());
			Assert.assertEquals(it1.next().toString(), it2.next().toString());
		}
		Assert.assertFalse(it2.hasNext());
	}

	private Iterator<CharSequence> removeDupe(List<IndexedNode> nodes) {
		return new MapIterator<>(
				CompressUtil.asNoDupeCharSequenceIterator(ExceptionIterator.of(nodes.iterator()).map(in -> {
					in.setNode(ByteString.of(in.getNode()));
					return in;
				}), (i, j, k) -> {}), IndexedNode::getNode);
	}
}
