package com.the_qa_company.qendpoint.core.dictionary.impl.section;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64;
import com.the_qa_company.qendpoint.core.util.string.ByteStringUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class PFCDictionarySectionBuilder {

	// FIXME: Due to java array indexes being int, only 2GB can be addressed per
	// dictionary section.
	private byte[] text = new byte[0]; // Encoded sequence
	private final int blocksize;
	private int numstrings;
	private final SequenceLog64 blocks;

	ByteArrayOutputStream byteOut = new ByteArrayOutputStream(16 * 1024);

	CharSequence previousStr = null;

	public PFCDictionarySectionBuilder(int blocksize, long numentries) {
		this.blocksize = blocksize;
		this.blocks = new SequenceLog64(32, numentries / blocksize);
		this.numstrings = 0;
	}

	public void add(CharSequence str) throws IOException {
		if (numstrings % blocksize == 0) {
			// Add new block pointer
			blocks.append(byteOut.size());

			// Copy full string
			ByteStringUtil.append(byteOut, str, 0);
		} else {
			// Find common part.
			int delta = ByteStringUtil.longestCommonPrefix(previousStr, str);
			// Write Delta in VByte
			VByte.encode(byteOut, delta);
			// Write remaining
			ByteStringUtil.append(byteOut, str, delta);
		}
		byteOut.write(0); // End of string

		numstrings++;
		previousStr = str;

	}

	public void finished() throws IOException {

		// Ending block pointer.
		blocks.append(byteOut.size());

		// Trim text/blocks
		blocks.aggressiveTrimToSize();

		byteOut.flush();
		text = byteOut.toByteArray();
	}

	public byte[] getText() {
		return text;
	}

	public int getBlocksize() {
		return blocksize;
	}

	public int getNumstrings() {
		return numstrings;
	}

	public SequenceLog64 getBlocks() {
		return blocks;
	}
}
