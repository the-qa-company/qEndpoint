package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.triples.IndexedNode;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Class to write a compress node file
 *
 * @author Antoine Willerval
 */
public class CompressNodeWriterRaw implements ICompressNodeWriter {
	private final CRCOutputStream out;

	public CompressNodeWriterRaw(OutputStream stream, long size) throws IOException {
		this.out = new CRCOutputStream(stream, new CRC8());
		IOUtil.writeLong(this.out, size);
		this.out.writeCRC();
		this.out.setCRC(new CRC32());
	}

	@Override
	public void appendNode(IndexedNode node) throws IOException {
		ByteString str = node.getNode();
		long index = node.getIndex();

		int len = str.length();
		IOUtil.writeInt(out, len);
		out.write(str.getBuffer(), 0, len);
		IOUtil.writeLong(out, index); // index of the node
	}

	@Override
	public void writeCRC() throws IOException {
		out.writeCRC();
	}

	@Override
	public void close() throws IOException {
		writeCRC();
		out.close();
	}
}
