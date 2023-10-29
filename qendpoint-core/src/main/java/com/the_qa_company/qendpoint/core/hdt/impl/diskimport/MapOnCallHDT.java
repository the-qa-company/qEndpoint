package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.hdt.HDTPrivate;
import com.the_qa_company.qendpoint.core.header.Header;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.triples.Triples;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;

/**
 * HDT implementation delaying the map method to avoid mapping into memory a
 * file if it's not required
 *
 * @author Antoine Willerval
 */
@SuppressWarnings("resource")
public class MapOnCallHDT implements HDTPrivate {
	private final Path hdtFile;
	private HDT hdt;

	public MapOnCallHDT(Path hdtFile) {
		// remove close suppress path files
		this.hdtFile = CloseSuppressPath.unpack(hdtFile.toAbsolutePath());
	}

	private HDT mapOrGetHDT() {
		if (hdt == null) {
			// map the HDT into memory
			try {
				hdt = HDTManager.mapHDT(hdtFile.toString());
			} catch (IOException e) {
				throw new RuntimeException("Can't map the hdt file", e);
			}
		}
		return hdt;
	}

	@Override
	public Header getHeader() {
		return mapOrGetHDT().getHeader();
	}

	@Override
	public Dictionary getDictionary() {
		return mapOrGetHDT().getDictionary();
	}

	@Override
	public Triples getTriples() {
		return mapOrGetHDT().getTriples();
	}

	@Override
	public void saveToHDT(OutputStream output, ProgressListener listener) throws IOException {
		Files.copy(hdtFile, output);
	}

	@Override
	public void saveToHDT(String fileName, ProgressListener listener) throws IOException {
		Path future = Path.of(fileName).toAbsolutePath();
		if (!future.equals(hdtFile)) {
			// copy file if not equals
			Files.copy(hdtFile, future, StandardCopyOption.REPLACE_EXISTING);
		}
		// otherwise, no need to copy a file if it's already there
	}

	@Override
	public long size() {
		return mapOrGetHDT().size();
	}

	@Override
	public String getBaseURI() {
		return mapOrGetHDT().getBaseURI();
	}

	@Override
	public void close() throws IOException {
		IOUtil.closeAll(hdt);
		hdt = null;
	}

	@Override
	public IteratorTripleString search(CharSequence subject, CharSequence predicate, CharSequence object)
			throws NotFoundException {
		return mapOrGetHDT().search(subject, predicate, object);
	}

	@Override
	public IteratorTripleString search(CharSequence subject, CharSequence predicate, CharSequence object,
			CharSequence graph) throws NotFoundException {
		return mapOrGetHDT().search(subject, predicate, object, graph);
	}

	@Override
	public IteratorTripleString search(TripleString triple) throws NotFoundException {
		return mapOrGetHDT().search(triple);
	}

	@Override
	public IteratorTripleString searchAll() throws NotFoundException {
		return mapOrGetHDT().searchAll();
	}

	@Override
	public IteratorTripleString search(CharSequence subject, CharSequence predicate, CharSequence object,
			int searchOrderMask) throws NotFoundException {
		return mapOrGetHDT().search(subject, predicate, object, searchOrderMask);
	}

	@Override
	public IteratorTripleString search(CharSequence subject, CharSequence predicate, CharSequence object,
			CharSequence graph, int searchOrderMask) throws NotFoundException {
		return mapOrGetHDT().search(subject, predicate, object, graph, searchOrderMask);
	}

	@Override
	public IteratorTripleString search(TripleString triple, int searchOrderMask) throws NotFoundException {
		return mapOrGetHDT().search(triple, searchOrderMask);
	}

	@Override
	public IteratorTripleString searchAll(int searchOrderMask) throws NotFoundException {
		return mapOrGetHDT().searchAll(searchOrderMask);
	}

	@Override
	public Iterator<TripleString> iterator() {
		return mapOrGetHDT().iterator();
	}

	@Override
	public void loadFromHDT(InputStream input, ProgressListener listener) throws IOException {
		((HDTPrivate) mapOrGetHDT()).loadFromHDT(input, listener);
	}

	@Override
	public void loadFromHDT(String fileName, ProgressListener listener) throws IOException {
		((HDTPrivate) mapOrGetHDT()).loadFromHDT(fileName, listener);
	}

	@Override
	public void mapFromHDT(File f, long offset, ProgressListener listener) throws IOException {
		((HDTPrivate) mapOrGetHDT()).mapFromHDT(f, offset, listener);
	}

	@Override
	public void loadOrCreateIndex(ProgressListener listener, HDTOptions disk) throws IOException {
		hdt = HDTManager.indexedHDT(hdt, listener, disk);
	}

	@Override
	public void populateHeaderStructure(String baseUri) {
		((HDTPrivate) mapOrGetHDT()).populateHeaderStructure(baseUri);
	}
}
