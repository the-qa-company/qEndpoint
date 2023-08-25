package com.the_qa_company.qendpoint.federation;

import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.repository.sparql.federation.SPARQLFederatedService;
import org.eclipse.rdf4j.repository.sparql.federation.SPARQLServiceResolver;

public class SPARQLServiceWikibaseLabelResolver extends SPARQLServiceResolver {
	private final TripleSource tripleSource;
	private final String userLocales;

	public SPARQLServiceWikibaseLabelResolver(TripleSource tripleSource, String userLocales) {
		this.tripleSource = tripleSource;
		this.userLocales = userLocales;
	}

	@Override
	protected FederatedService createService(String serviceUrl) throws QueryEvaluationException {
		// for the Wikibase url use a spacial FederatedService implementation
		if (serviceUrl.equals("http://wikiba.se/ontology#label")) {
			return new WikibaseLabelService(tripleSource, userLocales);
		} else {
			return new SPARQLFederatedService(serviceUrl, this.getHttpClientSessionManager());
		}

	}
}
