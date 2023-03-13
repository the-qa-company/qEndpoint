package com.the_qa_company.qendpoint.utils.rdf;

import com.fasterxml.jackson.core.JsonGenerator;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.query.QueryResultHandlerException;
import org.eclipse.rdf4j.query.resultio.sparqljson.SPARQLResultsJSONWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;

public class QEPSPARQLResultsJSONWriter extends SPARQLResultsJSONWriter {
	public interface IOExceptionConsumer<T> {
		void accept(T t) throws IOException;
	}

	public static final IRI JSON_RESULTS = Values.iri("http://www.w3.org/ns/formats/SPARQL_Results_JSON");
	private IOExceptionConsumer<JsonGenerator> componentBuilder;

	public QEPSPARQLResultsJSONWriter(OutputStream out) {
		super(out);
	}

	public QEPSPARQLResultsJSONWriter(Writer writer) {
		super(writer);
	}

	@Override
	public void startHeader() throws QueryResultHandlerException {
		if (!documentOpen) {
			startDocument();
		}

		if (!headerOpen && componentBuilder != null) {
			try {
				componentBuilder.accept(jg);
			} catch (IOException e) {
				throw new QueryResultHandlerException(e);
			}
		}

		super.startHeader();
	}

	public void setHeaderWriter(IOExceptionConsumer<JsonGenerator> componentBuilder) {
		this.componentBuilder = componentBuilder;
	}
}
