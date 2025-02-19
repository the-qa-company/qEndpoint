package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.triples.IndexedNode;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.ReplazableString;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Utility class to manipulate compressed node
 *
 * @author Antoine Willerval
 */
public class CompressUtil {
	/**
	 * the mask for shared computed compressed node
	 */
	public static final long SHARED_MASK = 1L;
	/**
	 * shift after the SHARED/DUPLICATES
	 */
	public static final int INDEX_SHIFT = 1;

	/**
	 * write a sorted list of indexed node
	 *
	 * @param strings  the nodes to write
	 * @param output   the output
	 * @param listener the listener to see the progress
	 * @throws IOException writing exception
	 */
	public static void writeCompressedSection(List<IndexedNode> strings, OutputStream output, ProgressListener listener)
			throws IOException {
		writeCompressedSection(ExceptionIterator.of(strings.iterator()), strings.size(), output, listener);
	}

	/**
	 * write a sorted iterator of indexed node
	 *
	 * @param it       iterator to write
	 * @param size     size of the iterator
	 * @param output   the output where to write
	 * @param listener the listener to see the progress
	 * @throws IOException writing exception
	 */
	public static void writeCompressedSection(ExceptionIterator<IndexedNode, IOException> it, long size,
			OutputStream output, ProgressListener listener) throws IOException {
		CompressNodeWriter writer = new CompressNodeWriter(output, size);
		long element = 0;
		long block = size < 10 ? 1 : size / 10;
		while (it.hasNext()) {
			if (listener != null && element % block == 0) {
				listener.notifyProgress((float) (10 * element / block), "write section " + element + "/" + size);
			}
			writer.appendNode(it.next());
			element++;
		}
		it.forEachRemaining(writer::appendNode);
		writer.writeCRC();
		if (listener != null) {
			listener.notifyProgress(100, "section completed " + size + " nodes");
		}
	}

	/**
	 * merge two stream together into an output stream
	 *
	 * @param stream1  input stream 1
	 * @param stream2  input stream 2
	 * @param output   output stream
	 * @param listener the listener to see the progress
	 * @throws IOException read/writing exception
	 */
	public static void mergeCompressedSection(InputStream stream1, InputStream stream2, OutputStream output,
			ProgressListener listener) throws IOException {
		CompressNodeReader in1r = new CompressNodeReader(stream1);
		CompressNodeReader in2r = new CompressNodeReader(stream2);

		long size1 = in1r.getSize();
		long size2 = in2r.getSize();

		// merge the section
		writeCompressedSection(new CompressNodeMergeIterator(in1r, in2r), size1 + size2, output, listener);
		// check we have completed the 2 readers
		in1r.checkComplete();
		in2r.checkComplete();
	}

	/**
	 * compute the shared-computed id from a shared-computable id
	 *
	 * @param id          the shared-computable id
	 * @param sharedCount the count of shared elements
	 * @return the shared-computed element
	 */
	public static long computeSharedNode(long id, long sharedCount) {
		if ((id & SHARED_MASK) != 0) {
			// shared element
			return CompressUtil.getId(id);
		}
		// not shared
		return CompressUtil.getId(id) + sharedCount;
	}

	/**
	 * convert this id to a shared-computable element
	 *
	 * @param id the id
	 * @return shared-computable element
	 */
	public static long asShared(long id) {
		return getHeaderId(id) | SHARED_MASK;
	}

	/**
	 * get the id from a header id
	 *
	 * @param headerId the header id
	 * @return the id
	 */
	public static long getId(long headerId) {
		return headerId >>> INDEX_SHIFT;
	}

	/**
	 * get a header id from an id
	 *
	 * @param id the id
	 * @return the header id
	 */
	public static long getHeaderId(long id) {
		return id << INDEX_SHIFT;
	}

	/**
	 * create a duplicate-free iterator from a sorted exception iterator, a
	 * callback for duplicated elements can be used
	 *
	 * @param nodes                  iterator sorted iterator
	 * @param duplicatedNodeConsumer duplicate callback
	 * @return a char sequence base iterator view of this iterator
	 */
	public static DuplicatedIterator asNoDupeCharSequenceIterator(ExceptionIterator<IndexedNode, ?> nodes,
			DuplicatedNodeConsumer duplicatedNodeConsumer) {
		return new DuplicatedIterator(nodes.asIterator(), duplicatedNodeConsumer);
	}

	/**
	 * Duplicate consumer for the
	 * {@link #asNoDupeCharSequenceIterator(ExceptionIterator, CompressUtil.DuplicatedNodeConsumer)}
	 * method
	 */
	@FunctionalInterface
	public interface DuplicatedNodeConsumer {
		/**
		 * called when the {@link CompressUtil.DuplicatedIterator} find a
		 * duplicated element
		 *
		 * @param originalIndex   the index id of the first element
		 * @param duplicatedIndex the index id of the duplicate element
		 * @param originalHeader  the header of the first element
		 */
		void onDuplicated(long originalIndex, long duplicatedIndex, long originalHeader);
	}

	public static class DuplicatedIterator implements Iterator<IndexedNode> {
		private final Iterator<IndexedNode> it;
		private final ReplazableString prev = new ReplazableString();
		private boolean prevRead = false;
		private IndexedNode next;
		private long id;
		private final DuplicatedNodeConsumer duplicatedNodeConsumer;
		private long lastHeader;

		DuplicatedIterator(Iterator<IndexedNode> it, DuplicatedNodeConsumer duplicatedNodeConsumer) {
			this.it = it;
			this.duplicatedNodeConsumer = Objects.requireNonNullElseGet(duplicatedNodeConsumer, () -> (i, j, k) -> {});
		}

		@Override
		public boolean hasNext() {
			if (next != null) {
				return true;
			}
			while (it.hasNext()) {
				IndexedNode node = it.next();
				ByteString next = node.getNode();
				boolean equals = prev.equals(next);

				assert prev.compareTo(next) <= 0 : "bad order : " + prev + " > " + next;
				assert !equals || prev.compareTo(next) == 0 : "bad equals : " + prev + " != " + next;
				assert equals || prev.compareTo(next) != 0 : "bad compare : " + prev + " != " + next;

				if (equals) {
					if (!prev.isEmpty() || prevRead) {
						// same as previous, ignore
						assert this.id != node.getIndex() : "same index and prevIndex";
						duplicatedNodeConsumer.onDuplicated(this.id, node.getIndex(), lastHeader);
						continue;
					} else {
						prevRead = true;
					}
				}
				this.next = node;
				prev.replace(next);
				this.id = node.getIndex();
				return true;
			}
			return false;
		}

		@Override
		public IndexedNode next() {
			if (!hasNext()) {
				return null;
			}
			IndexedNode old = next;
			next = null;
			return old;
		}

		public void setLastHeader(long lastHeader) {
			this.lastHeader = lastHeader;
		}
	}

	private CompressUtil() {
	}
}
