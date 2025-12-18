package com.the_qa_company.qendpoint.core.hdt.impl.converter;

import com.the_qa_company.qendpoint.core.dictionary.DictionaryPrivate;
import com.the_qa_company.qendpoint.core.hdt.Converter;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.impl.HDTImpl;
import com.the_qa_company.qendpoint.core.header.HeaderPrivate;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.Triples;
import com.the_qa_company.qendpoint.core.triples.TriplesFactory;
import com.the_qa_company.qendpoint.core.triples.TriplesPrivate;
import com.the_qa_company.qendpoint.core.triples.impl.OneReadTempTriples;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;

import java.io.IOException;
import java.nio.file.Path;

public class TriplesConverter implements Converter {
	private final String type;

	public TriplesConverter(String type) {
		this.type = type;
	}

	@Override
	public String getDestinationType() {
		return type;
	}

	@Override
	public void convertHDTFile(HDT origin, Path destination, ProgressListener listener, HDTOptions options)
			throws IOException {
		options = options.pushTop();
		options.set(HDTOptionsKeys.DICTIONARY_TYPE_KEY, origin.getDictionary().getType());

		int bufferSize = options.getInt32("bufferSize", 4096);

		try (CloseSuppressPath workingLocation = CloseSuppressPath
				.of(destination.resolveSibling(destination.getFileName() + "_workDir"));
				TriplesPrivate triples = TriplesFactory.createWriteTriples(options,
						workingLocation.resolve("tripleBitmap"), bufferSize,
						origin.getDictionary().supportGraphs() ? 1 : -1)) {

			HDTImpl impl = new HDTImpl((HeaderPrivate) origin.getHeader(), (DictionaryPrivate) origin.getDictionary(),
					triples, options);

			Triples triplesOrigin = origin.getTriples();
			IteratorTripleID it = triplesOrigin.searchAll();
			triples.load(new OneReadTempTriples(it, it.getOrder(), triplesOrigin.getNumberOfElements(), -1,
					origin.getDictionary().getNshared()), listener);

			impl.saveToHDT(destination, listener);
		}
	}
}
