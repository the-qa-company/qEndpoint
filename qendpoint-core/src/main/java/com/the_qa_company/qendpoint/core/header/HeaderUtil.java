package com.the_qa_company.qendpoint.core.header;

import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import com.the_qa_company.qendpoint.core.triples.TripleString;

public class HeaderUtil {

	private HeaderUtil() {
	}

	public static String cleanURI(CharSequence str) {
		String uri = str.toString();
		if (uri.length() >= 2 && uri.charAt(0) == '<' && uri.charAt(uri.length() - 1) == '>') {
			return uri.substring(1, uri.length() - 1);
		}
		return uri;
	}

	private static String cleanLiteral(String uri) {
		if (uri != null && uri.length() >= 2 && uri.charAt(0) == '"' && uri.charAt(uri.length() - 1) == '"') {
			return uri.substring(1, uri.length() - 1);
		}
		return uri;
	}

	public static String getProperty(Header header, String subject, String predicate) throws NotFoundException {
		IteratorTripleString it = header.search(cleanURI(subject), cleanURI(predicate), "");
		if (it.hasNext()) {
			TripleString ts = it.next();
			return ts.getObject().toString();
		}
		throw new NotFoundException();
	}

	public static long getPropertyLong(Header header, String subject, String predicate) throws NotFoundException {
		String str = HeaderUtil.getProperty(header, subject, predicate);
		try {
			return Long.parseLong(cleanLiteral(str));
		} catch (NumberFormatException ignored) {
		}
		throw new NotFoundException();
	}

	public static String getSubject(Header header, String predicate, String object) throws NotFoundException {
		IteratorTripleString it = header.search("", predicate, object);
		if (it.hasNext()) {
			TripleString ts = it.next();
			return ts.getObject().toString();
		}
		throw new NotFoundException();
	}

	public static String getBaseURI(Header header) throws NotFoundException {
		return HeaderUtil.getSubject(header, HDTVocabulary.RDF_TYPE, HDTVocabulary.HDT_DATASET);
	}
}
