package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.triples.IndexedNode;
import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.ByteStringUtil;
import com.the_qa_company.qendpoint.core.util.string.ReplazableString;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Class to write a compress node file
 *
 * @author Antoine Willerval
 */
public class CompressNodeWriter implements Closeable {
	private final CRCOutputStream out;
	private final ReplazableString previousStr = new ReplazableString();
	private final boolean stringLiterals;

	public CompressNodeWriter(OutputStream stream, long size, boolean stringLiterals) throws IOException {
		this.out = new CRCOutputStream(stream, new CRC8());
		VByte.encode(this.out, size);
		this.out.writeCRC();
		this.out.setCRC(new CRC32());
		this.stringLiterals = stringLiterals;
	}

	public void appendNode(IndexedNode node) throws IOException {
		ByteString str = node.getNode();
		long index = node.getIndex();

		// Find common part.
		int delta = ByteStringUtil.longestCommonPrefix(previousStr, str);
		previousStr.replace(str);
		// Write Delta in VByte
		VByte.encode(out, delta);
		if (stringLiterals) {
			ByteStringUtil.append(out, str, delta);
			out.write(0); // End of string
		} else {
			VByte.encode(out, str.length() - delta);
			IOUtil.writeBuffer(out, str.getBuffer(), delta, str.length() - delta, ProgressListener.ignore());
		}
		VByte.encode(out, index); // index of the node
	}

	public void writeCRC() throws IOException {
		out.writeCRC();
	}

	@Override
	public void close() throws IOException {
		writeCRC();
		out.close();
	}
}
