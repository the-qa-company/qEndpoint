package com.the_qa_company.qendpoint.core.dictionary.impl.section;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.compact.sequence.Sequence;
import com.the_qa_company.qendpoint.core.util.io.BigMappedByteBuffer;
import com.the_qa_company.qendpoint.core.util.string.CompactString;
import com.the_qa_company.qendpoint.core.util.string.ReplazableString;

import java.io.IOException;

/**
 * Performs extract keeping local state that can be reused to read consecutive
 * positions. Not thread friendly.
 *
 * @author Mario Arias
 */

public class PFCOptimizedExtractor {
	PFCDictionarySectionMap pfc;
	long numstrings;
	int blocksize;
	BigMappedByteBuffer[] buffers;
	BigMappedByteBuffer buffer;
	long[] posFirst;
	Sequence blocks;

	long bytebufferIndex = 0;
	ReplazableString tempString = new ReplazableString();
	long id = 0;

	public PFCOptimizedExtractor(PFCDictionarySectionMap pfc) {
		this.pfc = pfc;
		this.numstrings = pfc.numstrings;
		this.blocksize = pfc.blocksize;
		this.blocks = pfc.blocks;
		this.posFirst = pfc.posFirst;

		this.buffers = pfc.buffers;
		if (numstrings > 0 && this.buffers != null && this.buffers.length > 0) {
			this.buffer = buffers[0].duplicate();
		} else {
			if (this.buffers == null) {
				System.err.println("Warning: Mapping a PFC section with null buffers. " + numstrings + " / " + blocksize
						+ " / " + pfc.dataSize + " / " + pfc.blocks.getNumberOfElements());
			} else if (this.buffers.length == 0) {
				System.err.println(
						"Warning: Mapping a PFC section with buffers but no entries. " + numstrings + " / " + blocksize
								+ " / " + pfc.dataSize + " / " + pfc.blocks.getNumberOfElements() + " / " + buffers);
			}
			this.numstrings = 0;
		}
	}

	public CharSequence extract(long target) {
		if (target < 1 || target > numstrings) {
			throw new IndexOutOfBoundsException(
					"Trying to access position " + target + " but PFC has " + numstrings + " elements.");
		}

		if (target > id && target < ((id % blocksize) + blocksize)) {
			// If the searched string is in the current block, just continue

			while (id < target) {
				if (!buffer.hasRemaining()) {
					buffer = buffers[(int) ++bytebufferIndex].duplicate();
					buffer.rewind();
				}
				try {
					if ((id % blocksize) == 0) {
						tempString.replace(buffer, 0);
					} else {
						long delta = VByte.decode(buffer);
						tempString.replace(buffer, (int) delta);
					}
					id++;

					if (id == target) {
						return new CompactString(tempString).getDelayed();
						// return tempString.toString();
					}
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
			// Should not reach here.
			System.err.println("Not found: " + target + " out of " + this.getNumStrings());
			return null;

		} else {
			// The searched string is in another block, seek directly to that
			// one.

			id = target;

			long block = (target - 1) / blocksize;
			bytebufferIndex = block / PFCDictionarySectionMap.BLOCKS_PER_BYTEBUFFER;
			buffer = buffers[(int) bytebufferIndex++].duplicate();
			buffer.position((int) (blocks.get(block)
					- posFirst[(int) (block / PFCDictionarySectionMap.BLOCKS_PER_BYTEBUFFER)]));

			try {
				tempString = new ReplazableString();
				tempString.replace(buffer, 0);

				long stringid = (target - 1) % blocksize;
				for (long i = 0; i < stringid; i++) {
					long delta = VByte.decode(buffer);
					tempString.replace(buffer, (int) delta);
				}
				return new CompactString(tempString).getDelayed();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public long getNumStrings() {
		return numstrings;
	}

}
