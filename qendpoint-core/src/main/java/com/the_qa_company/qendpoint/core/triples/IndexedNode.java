package com.the_qa_company.qendpoint.core.triples;

import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.CompactString;

public class IndexedNode implements Comparable<IndexedNode>, Cloneable {
	private ByteString node;
	private long index;

	public IndexedNode(ByteString node, long index) {
		this.node = node;
		this.index = index;
	}

	public IndexedNode(CharSequence node, long index) {
		this(ByteString.of(node), index);
	}

	public IndexedNode() {
	}

	public ByteString getNode() {
		return node;
	}

	public long getIndex() {
		return index;
	}

	public void setIndex(long index) {
		this.index = index;
	}

	public void setNode(ByteString node) {
		this.node = node;
	}

	@Override
	public int compareTo(IndexedNode o) {
		return node.compareTo(o.node);
	}

	@Override
	public IndexedNode clone() {
		try {
			IndexedNode clone = (IndexedNode) super.clone();
			clone.node = new CompactString(node);
			return clone;
		} catch (CloneNotSupportedException e) {
			throw new AssertionError(e);
		}
	}

}
