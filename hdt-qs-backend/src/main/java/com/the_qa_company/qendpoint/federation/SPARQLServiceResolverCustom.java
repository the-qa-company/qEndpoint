package com.the_qa_company.qendpoint.federation;

import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.repository.sparql.federation.SPARQLFederatedService;
import org.eclipse.rdf4j.repository.sparql.federation.SPARQLServiceResolver;

public class SPARQLServiceResolverCustom extends SPARQLServiceResolver {
	TripleSource tripleSource;

	public SPARQLServiceResolverCustom(TripleSource tripleSource) {
		this.tripleSource = tripleSource;
	}

	@Override
	protected FederatedService createService(String serviceUrl) throws QueryEvaluationException {
		// for the Wikibase url use a spacial FederatedService implementation
		if (serviceUrl.equals("http://wikiba.se/ontology#label")) {
			return new WikibaseLabelService(tripleSource);
		} else {
			return new SPARQLFederatedService(serviceUrl, this.getHttpClientSessionManager());
		}

	}
}
