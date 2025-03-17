package com.the_qa_company.qendpoint.core.hdt.impl;

import com.the_qa_company.qendpoint.core.dictionary.DictionaryFactory;
import com.the_qa_company.qendpoint.core.dictionary.DictionaryPrivate;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.header.HeaderFactory;
import com.the_qa_company.qendpoint.core.header.HeaderPrivate;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import com.the_qa_company.qendpoint.core.triples.TriplesPrivate;
import com.the_qa_company.qendpoint.core.triples.impl.WriteBitmapTriples;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * HDT implementation to write on disk the components
 *
 * @author Antoine Willerval
 */
public class WriteHDTImpl extends HDTBase<HeaderPrivate, DictionaryPrivate, TriplesPrivate> {
	private String baseURI;
	private final CloseSuppressPath workingLocation;
	private boolean isClosed;

	public WriteHDTImpl(HDTOptions spec, CloseSuppressPath workingLocation, int bufferSize) throws IOException {
		super(spec);
		this.workingLocation = workingLocation;
		workingLocation.mkdirs();

		dictionary = DictionaryFactory.createWriteDictionary(this.spec, workingLocation.resolve("section"), bufferSize);
		// we need to have the bitmaps in memory, so we can't bypass the
		// implementation
		triples = new WriteBitmapTriples(this.spec, workingLocation.resolve("tripleBitmap"), bufferSize,
				dictionary.supportGraphs() ? 1 : -1);
		// small, can use default implementation
		header = HeaderFactory.createHeader(this.spec);
	}

	public WriteHDTImpl(HDTOptions spec, CloseSuppressPath workingLocation, DictionaryPrivate dict,
			TriplesPrivate triples, HeaderPrivate header) throws IOException {
		super(spec);
		this.workingLocation = workingLocation;
		workingLocation.mkdirs();

		dictionary = dict;
		// we need to have the bitmaps in memory, so we can't bypass the
		// implementation
		this.triples = triples;
		// small, can use default implementation
		this.header = header;
	}

	@Override
	public void setBaseUri(String baseURI) {
		this.baseURI = baseURI;
	}

	@Override
	public void loadFromHDT(InputStream input, ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public void loadFromHDT(String fileName, ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public void mapFromHDT(File f, long offset, ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public void loadOrCreateIndex(ProgressListener listener, HDTOptions disk) {
		throw new NotImplementedException();
	}

	@Override
	public void saveToHDT(String fileName, ProgressListener listener) throws IOException {
		try (OutputStream out = new FastBufferedOutputStream(Files.newOutputStream(Path.of(fileName)))) {
			saveToHDT(out, listener);
		}
	}

	@Override
	public long size() {
		if (isClosed)
			return 0;

		return getDictionary().size() + getTriples().size();
	}

	@Override
	public String getBaseURI() {
		return baseURI;
	}

	@Override
	public boolean isClosed() {
		return isClosed;
	}

	@Override
	public void close() throws IOException {
		if (isClosed()) {
			return;
		}
		isClosed = true;
		IOUtil.closeAll(dictionary, triples, workingLocation);
	}

	@Override
	public IteratorTripleString search(CharSequence subject, CharSequence predicate, CharSequence object) {
		throw new NotImplementedException();
	}

	@Override
	public IteratorTripleString search(CharSequence subject, CharSequence predicate, CharSequence object,
			int searchOrderMask) throws NotFoundException {
		throw new NotImplementedException();
	}

}
