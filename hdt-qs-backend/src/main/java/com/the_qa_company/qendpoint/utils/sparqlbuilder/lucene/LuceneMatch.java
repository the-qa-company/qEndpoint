package com.the_qa_company.qendpoint.utils.sparqlbuilder.lucene;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.sail.lucene.LuceneSailSchema;
import org.eclipse.rdf4j.sparqlbuilder.core.Prefix;
import org.eclipse.rdf4j.sparqlbuilder.core.SparqlBuilder;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.GraphPattern;
import org.eclipse.rdf4j.sparqlbuilder.rdf.Rdf;
import org.eclipse.rdf4j.sparqlbuilder.rdf.RdfBlankNode;
import org.eclipse.rdf4j.sparqlbuilder.rdf.RdfObject;
import org.eclipse.rdf4j.sparqlbuilder.rdf.RdfPredicate;
import org.eclipse.rdf4j.sparqlbuilder.rdf.RdfResource;
import org.eclipse.rdf4j.sparqlbuilder.rdf.RdfSubject;

/**
 * Graph pattern to create match object for SPARQL queries
 *
 * @author Antoine Willerval
 */
public class LuceneMatch implements GraphPattern {
	/**
	 * the lucene search namespace prefix
	 */
	public static final Prefix NS = SparqlBuilder.prefix(new SimpleNamespace("search", LuceneSailSchema.NAMESPACE));

	/**
	 * create a match object from a subject
	 * @param subject the subject to match
	 * @return LuceneMatch object
	 */
	public static LuceneMatch of(RdfSubject subject) {
		return new LuceneMatch(subject);
	}
	private RdfBlankNode.PropertiesBlankNode matchesObject;
	private final RdfSubject subject;

	/**
	 * create a match object from a subject
	 * @param subject the subject to match
	 */
	public LuceneMatch(RdfSubject subject) {
		this.subject = subject;
	}

	/**
	 * add a custom (predicate, object) to match with the match (can be used for future/custom Lucene options)
	 * @param predicate predicate
	 * @param object object
	 * @return this
	 */
	public LuceneMatch customMatches(RdfPredicate predicate, RdfObject object) {
		if (matchesObject == null) {
			matchesObject = Rdf.bNode(predicate, object);
		} else {
			matchesObject = matchesObject.andHas(predicate, object);
		}
		return this;
	}

	private LuceneMatch addHas(IRI predicate, RdfObject object) {
		return customMatches(Rdf.iri(predicate), object);
	}

	/**
	 * specify the search:query value
	 * @param searchTerms search query
	 * @return this
	 */
	public LuceneMatch query(String searchTerms) {
		return addHas(LuceneSailSchema.QUERY, Rdf.literalOf(searchTerms));
	}

	/**
	 * specify the search:property value
	 * @param property query property
	 * @return this
	 */
	public LuceneMatch property(RdfResource property) {
		return addHas(LuceneSailSchema.PROPERTY, property);
	}

	/**
	 * specify the search:score value
	 * @param score query score
	 * @return this
	 */
	public LuceneMatch score(RdfResource score) {
		return addHas(LuceneSailSchema.SCORE, score);
	}

	/**
	 * specify the search:snippet value
	 * @param snippet query snippet
	 * @return this
	 */
	public LuceneMatch snippet(RdfResource snippet) {
		return addHas(LuceneSailSchema.SNIPPET, snippet);
	}

	/**
	 * specify the search:indexid value
	 * @param indexId query lucene index id
	 * @return this
	 */
	public LuceneMatch indexId(RdfResource indexId) {
		return addHas(LuceneSailSchema.INDEXID, indexId);
	}

	@Override
	public String getQueryString() {
		RdfObject matches;
		if (matchesObject == null) {
			matches = Rdf.bNode();
		} else {
			matches = matchesObject;
		}
		return subject.has(LuceneSailSchema.MATCHES, matches).getQueryString();
	}
}
