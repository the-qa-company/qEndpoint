package com.the_qa_company.qendpoint.core.hdt.writer;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.zip.GZIPOutputStream;

import com.the_qa_company.qendpoint.core.rdf.TripleWriter;
import com.the_qa_company.qendpoint.core.storage.TempBuffOut;
import com.the_qa_company.qendpoint.core.triples.TripleString;

public class TripleWriterNtriples implements TripleWriter {

	private final Writer out;
	private boolean close = false;

	public TripleWriterNtriples(String outFile, boolean compress) throws IOException {
		if (compress) {
			this.out = new OutputStreamWriter(new GZIPOutputStream(new TempBuffOut(new FileOutputStream(outFile))));
		} else {
			this.out = new BufferedWriter(new FileWriter(outFile));
		}
		close = true;
	}

	public TripleWriterNtriples(OutputStream out) {
		this.out = new BufferedWriter(new OutputStreamWriter(out));
	}

	@Override
	public void addTriple(TripleString str) throws IOException {
		str.dumpNtriple(out);
	}

	@Override
	public void close() throws IOException {
		if (close) {
			out.close();
		} else {
			out.flush();
		}
	}

}
