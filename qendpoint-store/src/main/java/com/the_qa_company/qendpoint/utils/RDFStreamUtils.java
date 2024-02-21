package com.the_qa_company.qendpoint.utils;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;

/**
 * Utility class to help to handle RDF stream
 *
 * @author Antoine Willerval
 */
public class RDFStreamUtils {
	private static final Map<RDFFormat, RDFNotation> lookupRH = new HashMap<>();
	private static final Map<RDFNotation, RDFFormat> lookupHR = new HashMap<>();

	static {
		// register notations
		registerNotation(RDFNotation.RDFXML, RDFFormat.RDFXML);
		registerNotation(RDFNotation.NTRIPLES, RDFFormat.NTRIPLES);
		registerNotation(RDFNotation.TURTLE, RDFFormat.TURTLE);
		registerNotation(RDFNotation.N3, RDFFormat.N3);
		registerNotation(RDFNotation.NQUAD, RDFFormat.NQUADS);
		registerNotation(RDFNotation.TRIG, RDFFormat.TRIG);
		registerNotation(RDFNotation.TRIX, RDFFormat.TRIX);
		registerNotation(RDFNotation.JSONLD, RDFFormat.JSONLD);
	}

	/**
	 * convert this stream into a CompressorInputStream if the file type is .gz,
	 * .tgz, .bz, .bz2 or .xz, otherwise it returns the stream
	 *
	 * @param stream   the stream to uncompress
	 * @param filename the filename associate with the stream
	 * @return stream
	 * @throws IOException if the CompressorInputStream can't be created
	 */
	public static InputStream uncompressedStream(InputStream stream, String filename) throws IOException {
		String name = filename.toLowerCase();
		if (name.endsWith(".gz") || name.endsWith(".tgz")) {
			return new GZIPInputStream(stream);
		} else if (name.endsWith("bz2") || name.endsWith("bz")) {
			return new BZip2CompressorInputStream(stream, true);
		} else if (name.endsWith("xz")) {
			return new XZCompressorInputStream(stream, true);
		} else {
			return stream;
		}
	}

	/**
	 * read a stream of a certain type to {@link Statement}
	 *
	 * @param stream            rdf stream
	 * @param format            format of the stream
	 * @param keepBNode         keep blank node
	 * @param statementConsumer the triple consumer
	 * @throws IOException io error
	 */
	public static void readRDFStream(InputStream stream, RDFFormat format, boolean keepBNode,
			Consumer<Statement> statementConsumer) throws IOException {
		RDFParser parser = Rio.createParser(format);
		parser.setPreserveBNodeIDs(keepBNode);
		parser.setRDFHandler(new RDFHandler() {
			@Override
			public void startRDF() throws RDFHandlerException {
			}

			@Override
			public void endRDF() throws RDFHandlerException {
			}

			@Override
			public void handleNamespace(String s, String s1) throws RDFHandlerException {
			}

			@Override
			public void handleStatement(Statement statement) throws RDFHandlerException {
				statementConsumer.accept(statement);
			}

			@Override
			public void handleComment(String s) throws RDFHandlerException {

			}
		});
		parser.parse(stream);
	}

	/**
	 * create an iterator from a RDF inputstream
	 *
	 * @param stream    rdf stream
	 * @param format    format of the stream
	 * @param keepBNode keep blank node
	 * @return the iterator
	 */
	public static Iterator<Statement> readRDFStreamAsIterator(InputStream stream, RDFFormat format, boolean keepBNode) {
		return PipedIterator.createOfCallback(pipe -> readRDFStream(stream, format, keepBNode, pipe::addElement));
	}

	/**
	 * create an iterator from a RDF inputstream
	 *
	 * @param stream    rdf stream
	 * @param format    format of the stream
	 * @param keepBNode keep blank node
	 * @return the iterator
	 */
	public static Iterator<TripleString> readRDFStreamAsTripleStringIterator(InputStream stream, RDFFormat format,
			boolean keepBNode) {
		return new MapIterator<>(readRDFStreamAsIterator(stream, format, keepBNode),
				statement -> new TripleString(statement.getSubject().toString(), statement.getPredicate().toString(),
						statement.getObject().toString()));
	}

	/**
	 * Convert a CharSequence containing a component into a {@link Value}
	 *
	 * @param vf  value factory
	 * @param seq component
	 * @return value
	 */
	public static Value convertCharSequence(ValueFactory vf, CharSequence seq) {
		if (seq == null || seq.isEmpty()) {
			return null;
		}

		switch (seq.charAt(0)) {
		case '_' -> {
			// bnode
			if (seq.length() <= 2 || seq.charAt(1) != ':') {
				throw new IllegalArgumentException("Bad BNode sequence: " + seq);
			}
			return vf.createBNode(seq.subSequence(2, seq.length()).toString());
		}
		case '"' -> {
			// literal
			if (seq.length() < 2) {
				throw new IllegalArgumentException("Bad literal: " + seq);
			}

			int typeIndex = LiteralsUtils.getTypeIndex(seq);
			if (typeIndex == -1) {
				int langIndex = LiteralsUtils.getLangIndex(seq);
				if (langIndex == -1) {
					if (seq.charAt(seq.length() - 1) != '"') {
						throw new IllegalArgumentException("Bad literal: " + seq);
					}

					return vf.createLiteral(seq.subSequence(1, seq.length() - 1).toString());
				}

				String lit = seq.subSequence(1, langIndex - 2).toString();
				String lang = seq.subSequence(langIndex, seq.length()).toString();
				return vf.createLiteral(lit, lang);
			}
			String lit = seq.subSequence(1, typeIndex - 3).toString();
			String type = seq.subSequence(typeIndex + 1, seq.length() - 1).toString();

			return vf.createLiteral(lit, vf.createIRI(type));
		}
		case '<' -> {
			// iri
			if (seq.length() < 2 || seq.charAt(seq.length() - 1) != '>') {
				throw new IllegalArgumentException("Bad iri: " + seq);
			}
			return vf.createIRI(seq.subSequence(1, seq.length() - 1).toString());
		}
		default -> {
			return vf.createIRI(seq.toString());
		}
		}
	}

	/**
	 * Convert HDT {@link TripleString} to {@link Statement}
	 *
	 * @param vf     value factor
	 * @param string HDT triple
	 * @return statement
	 */
	public static Statement convertStatement(ValueFactory vf, TripleString string) {
		Value s = convertCharSequence(vf, string.getSubject());
		Value p = convertCharSequence(vf, string.getPredicate());
		Value o = convertCharSequence(vf, string.getObject());

		if (!s.isResource()) {
			throw new IllegalArgumentException("Triple subject isn't a resource: " + string.getSubject());
		}

		if (!p.isIRI()) {
			throw new IllegalArgumentException("Triple predicate isn't an iri: " + string.getPredicate());
		}

		return vf.createStatement((Resource) s, (IRI) p, o);
	}

	/**
	 * register notation for {@link #hdtToRDF4JNotation(RDFNotation)} and
	 * {@link #rdf4jToHDTNotation(RDFFormat)}.
	 *
	 * @param hdt hdt notation
	 * @param rio rdf4j notation
	 */
	public static void registerNotation(RDFNotation hdt, RDFFormat rio) {
		RDFNotation lr = lookupRH.put(rio, hdt);
		RDFFormat lh = lookupHR.put(hdt, rio);

		assert (lr == null || lr == hdt) && (lh == null || lh == rio) : "registered twice with different values";
	}

	/**
	 * hdt {@link RDFNotation} to RDF4J {@link RDFFormat}
	 *
	 * @param notation hdt notation
	 * @return rdf4j notation
	 */
	public static RDFFormat hdtToRDF4JNotation(RDFNotation notation) {
		RDFFormat rdfNotation = lookupHR.get(notation);
		if (rdfNotation != null) {
			return rdfNotation;
		}
		throw new IllegalArgumentException("Unknown notation: " + notation);
	}

	/**
	 * hdt {@link RDFNotation} to RDF4J {@link RDFFormat}
	 *
	 * @param notation hdt notation
	 * @return rdf4j notation
	 */
	public static RDFNotation rdf4jToHDTNotation(RDFFormat notation) {
		RDFNotation rdfNotation = lookupRH.get(notation);
		if (rdfNotation != null) {
			return rdfNotation;
		}
		throw new IllegalArgumentException("Unknown notation: " + notation);
	}

	private RDFStreamUtils() {
	}
}
