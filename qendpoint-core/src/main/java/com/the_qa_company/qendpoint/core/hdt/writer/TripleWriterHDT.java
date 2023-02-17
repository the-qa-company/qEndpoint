package com.the_qa_company.qendpoint.core.hdt.writer;

import com.the_qa_company.qendpoint.core.dictionary.TempDictionary;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.hdt.TempHDT;
import com.the_qa_company.qendpoint.core.hdt.impl.HDTImpl;
import com.the_qa_company.qendpoint.core.hdt.impl.ModeOfLoading;
import com.the_qa_company.qendpoint.core.hdt.impl.TempHDTImpl;
import com.the_qa_company.qendpoint.core.header.HeaderUtil;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.rdf.TripleWriter;
import com.the_qa_company.qendpoint.core.triples.TempTriples;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.StopWatch;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

public class TripleWriterHDT implements TripleWriter {

	private final OutputStream out;
	private boolean close = false;
	HDTOptions spec;
	String baseUri;

	StopWatch st = new StopWatch();
	TempHDT modHDT;
	TempDictionary dictionary;
	TempTriples triples;
	long num = 0;
	long size = 0;

	public TripleWriterHDT(String baseUri, HDTOptions spec, String outFile, boolean compress) throws IOException {
		this.baseUri = baseUri;
		this.spec = spec;
		if (compress) {
			this.out = new BufferedOutputStream(
					new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(outFile))));
		} else {
			this.out = new BufferedOutputStream(new FileOutputStream(outFile));
		}
		close = true;
		init();
	}

	public TripleWriterHDT(String baseUri, HDTOptions spec, OutputStream out) {
		this.baseUri = baseUri;
		this.spec = spec;
		this.out = new BufferedOutputStream(out);
		init();
	}

	private void init() {
		// Create TempHDT
		modHDT = new TempHDTImpl(spec, baseUri, ModeOfLoading.ONE_PASS);
		dictionary = modHDT.getDictionary();
		triples = modHDT.getTriples();

		// Load RDF in the dictionary and generate triples
		dictionary.startProcessing();
	}

	@Override
	public void addTriple(TripleString triple) throws IOException {
		triples.insert(dictionary.insert(triple.getSubject(), TripleComponentRole.SUBJECT),
				dictionary.insert(triple.getPredicate(), TripleComponentRole.PREDICATE),
				dictionary.insert(triple.getObject(), TripleComponentRole.OBJECT));
		num++;
		size += triple.getSubject().length() + triple.getPredicate().length() + triple.getObject().length() + 4; // Spaces
																													// and
																													// final
																													// dot
	}

	@Override
	public void close() throws IOException {
		ProgressListener listener = null;

		dictionary.endProcessing();

		// Reorganize both the dictionary and the triples
		modHDT.reorganizeDictionary(listener);
		modHDT.reorganizeTriples(listener);

		modHDT.getHeader().insert("_:statistics", HDTVocabulary.ORIGINAL_SIZE, size);

		// Convert to HDT
		HDTImpl hdt = new HDTImpl(spec);
		hdt.loadFromModifiableHDT(modHDT, listener);
		hdt.populateHeaderStructure(modHDT.getBaseURI());

		// Add file size to Header
		try {
			long originalSize = HeaderUtil.getPropertyLong(modHDT.getHeader(), "_:statistics",
					HDTVocabulary.ORIGINAL_SIZE);
			hdt.getHeader().insert("_:statistics", HDTVocabulary.ORIGINAL_SIZE, originalSize);
		} catch (NotFoundException e) {
		}

		modHDT.close();

		hdt.saveToHDT(out, listener);
		hdt.close();

		if (close) {
			out.close();
		} else {
			out.flush();
		}
	}

}
