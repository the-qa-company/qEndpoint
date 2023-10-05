package com.the_qa_company.qendpoint.core.hdt.impl;

import com.the_qa_company.qendpoint.core.dictionary.DictionaryPrivate;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.hdt.HDTPrivate;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.header.Header;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.ControlInfo;
import com.the_qa_company.qendpoint.core.options.ControlInformation;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import com.the_qa_company.qendpoint.core.triples.TriplesPrivate;
import com.the_qa_company.qendpoint.core.util.StringUtil;
import com.the_qa_company.qendpoint.core.util.listener.IntermediateListener;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;

/**
 * Abstract hdt base for {@link HDTPrivate}
 *
 * @param <H> header type
 * @param <D> dictionary type
 * @param <T> triple type
 */
public abstract class HDTBase<H extends Header, D extends DictionaryPrivate, T extends TriplesPrivate>
		implements HDTPrivate {
	protected final HDTOptions spec;
	protected H header;
	protected D dictionary;
	protected T triples;

	protected HDTBase(HDTOptions spec) {
		this.spec = HDTOptions.ofNullable(spec);
	}

	/**
	 * set the base URI of the hdt
	 *
	 * @param baseURI base uri
	 */
	public abstract void setBaseUri(String baseURI);

	/**
	 * @return if the HDT is closed
	 */
	public abstract boolean isClosed();

	/*
	 * (non-Javadoc)
	 * @see hdt.HDT#getHeader()
	 */
	@Override
	public H getHeader() {
		return header;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.HDT#getDictionary()
	 */
	@Override
	public D getDictionary() {
		return dictionary;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.HDT#getTriples()
	 */
	@Override
	public T getTriples() {
		return triples;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.hdt.HDT#getSize()
	 */
	@Override
	public long size() {
		if (isClosed())
			return 0;

		return dictionary.size() + triples.size();
	}

	public HDTOptions getSpec() {
		return spec;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.HDT#saveToHDT(java.io.OutputStream)
	 */
	@Override
	public void saveToHDT(OutputStream output, ProgressListener listener) throws IOException {
		ControlInfo ci = new ControlInformation();
		IntermediateListener iListener = new IntermediateListener(listener);

		ci.clear();
		ci.setType(ControlInfo.Type.GLOBAL);
		ci.setFormat(HDTVocabulary.HDT_CONTAINER);
		ci.save(output);

		ci.clear();
		ci.setType(ControlInfo.Type.HEADER);
		header.save(output, ci, iListener);

		ci.clear();
		ci.setType(ControlInfo.Type.DICTIONARY);
		dictionary.save(output, ci, iListener);

		ci.clear();
		ci.setType(ControlInfo.Type.TRIPLES);
		triples.save(output, ci, iListener);
	}

	public static long getRawSize(Header header) {

		try {
			IteratorTripleString rawSize1 = header.search("_:statistics", HDTVocabulary.ORIGINAL_SIZE, "");
			if (!rawSize1.hasNext()) {
				return -1;
			}

			CharSequence obj = rawSize1.next().getObject();
			// remove "s in "<long>"
			return Long.parseLong(obj, 1, obj.length() - 1, 10);
		} catch (NotFoundException e) {
			return -1;
		}
	}

	@Override
	public void populateHeaderStructure(String baseUri) {
		if (baseUri == null || baseUri.length() == 0) {
			throw new IllegalArgumentException("baseURI cannot be empty");
		}

		if (isClosed()) {
			throw new IllegalStateException("Cannot add header to a closed HDT.");
		}

		H header = getHeader();
		D dictionary = getDictionary();
		T triples = getTriples();
		header.insert(baseUri, HDTVocabulary.RDF_TYPE, HDTVocabulary.HDT_DATASET);
		header.insert(baseUri, HDTVocabulary.RDF_TYPE, HDTVocabulary.VOID_DATASET);

		// VOID
		header.insert(baseUri, HDTVocabulary.VOID_TRIPLES, triples.getNumberOfElements());
		header.insert(baseUri, HDTVocabulary.VOID_PROPERTIES, dictionary.getNpredicates());
		header.insert(baseUri, HDTVocabulary.VOID_DISTINCT_SUBJECTS, dictionary.getNsubjects());
		header.insert(baseUri, HDTVocabulary.VOID_DISTINCT_OBJECTS, dictionary.getNobjects());
		if (dictionary.supportGraphs()) {
			header.insert(baseUri, HDTVocabulary.VOID_DISTINCT_GRAPHS, dictionary.getNgraphs());
		}

		// Structure
		String formatNode = "_:format";
		String dictNode = "_:dictionary";
		String triplesNode = "_:triples";
		String statisticsNode = "_:statistics";
		String publicationInfoNode = "_:publicationInformation";

		header.insert(baseUri, HDTVocabulary.HDT_FORMAT_INFORMATION, formatNode);
		header.insert(formatNode, HDTVocabulary.HDT_DICTIONARY, dictNode);
		header.insert(formatNode, HDTVocabulary.HDT_TRIPLES, triplesNode);
		header.insert(baseUri, HDTVocabulary.HDT_STATISTICAL_INFORMATION, statisticsNode);
		header.insert(baseUri, HDTVocabulary.HDT_PUBLICATION_INFORMATION, publicationInfoNode);

		dictionary.populateHeader(header, dictNode);
		triples.populateHeader(header, triplesNode);

		header.insert(statisticsNode, HDTVocabulary.HDT_SIZE, getDictionary().size() + getTriples().size());

		// Current time
		header.insert(publicationInfoNode, HDTVocabulary.DUBLIN_CORE_ISSUED, StringUtil.formatDate(new Date()));
	}

}
