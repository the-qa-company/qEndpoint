package com.the_qa_company.qendpoint.federation;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.SKOS;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.query.impl.ListBindingSet;
import org.eclipse.rdf4j.sail.SailException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * This implements a federated service that mimics the Wikibase query service
 * SERVICE clause <http://wikiba.se/ontology#label>, this is achieved by
 * overwriting the standard SERVICE clause for this particular url check
 * https://en.wikibooks.org/wiki/SPARQL/SERVICE_-_Label for a full description
 * of the specification
 */
public class WikibaseLabelService implements FederatedService {

	static final ValueFactory vf = SimpleValueFactory.getInstance();
	TripleSource tripleSource;

	WikibaseLabelService(TripleSource tripleSource) {
		this.tripleSource = tripleSource;
	}

	@Override
	public boolean ask(Service service, BindingSet bindingSet, String s) throws QueryEvaluationException {
		System.out.println("Should never pass here, report the query if this is the case!");
		return false;
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> select(Service service, Set<String> set,
			BindingSet bindingSet, String s) throws QueryEvaluationException {
		System.out.println("Should never pass here, report the query if this is the case!");
		return null;
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(Service service,
			CloseableIteration<BindingSet, QueryEvaluationException> closeableIteration, String s)
			throws QueryEvaluationException {
		TupleExpr tupleExpr = service.getArg();
		// currently implements only the automatic mode
		// https://en.wikibooks.org/wiki/SPARQL/SERVICE_-_Label
		// the SERVICE clause contains only one triple pattern that specifies
		// the language
		if (tupleExpr instanceof StatementPattern) {
			StatementPattern statement = (StatementPattern) tupleExpr;
			// the predicate must be <http://wikiba.se/ontology#language>
			if (statement.getPredicateVar().getValue().stringValue().equals("http://wikiba.se/ontology#language")) {
				// the object must be a literal without language tag and without
				// datatype
				if (statement.getObjectVar().getValue().isLiteral()) {
					Literal literal = (Literal) statement.getObjectVar().getValue();
					if (!literal.getLanguage().isPresent() && literal.getDatatype() == XSD.STRING) {
						List<String> languages = Arrays.asList(literal.getLabel().split(","));
						if (languages.size() > 0) {
							return new CloseableIteration<BindingSet, QueryEvaluationException>() {
								@Override
								public void close() throws QueryEvaluationException {
									closeableIteration.close();
								}

								@Override
								public boolean hasNext() throws QueryEvaluationException {
									return closeableIteration.hasNext();
								}

								@Override
								public BindingSet next() throws QueryEvaluationException {
									return expandBindingSet(closeableIteration.next(), languages);
								}

								@Override
								public void remove() throws QueryEvaluationException {
									closeableIteration.remove();
								}
							};
						}
					}

				}
			}
		}
		throw new QueryEvaluationException();
	}

	private BindingSet expandBindingSet(BindingSet bindingSet, List<String> languages) {
		ArrayList<String> namesWithLabels = new ArrayList<>();
		ArrayList<Value> valuesWithLabels = new ArrayList<>();
		// according to https://en.wikibooks.org/wiki/SPARQL/SERVICE_-_Label 3
		// properties have to be expanded
		IRI[] expantionProperties = new IRI[] { RDFS.LABEL, SKOS.ALT_LABEL,
				vf.createIRI("https://schema.org/description") };
		String[] expantionNameSuffix = new String[] { "Label", "AltLabel", "Description" };
		for (int e = 0; e < 3; e++) {
			String[] languageLabels = new String[languages.size()];
			for (String name : bindingSet.getBindingNames()) {
				namesWithLabels.add(name);
				valuesWithLabels.add(bindingSet.getBinding(name).getValue());
				namesWithLabels.add(name + expantionNameSuffix[e]);
				if (bindingSet.getBinding(name).getValue() instanceof Resource) {
					try (CloseableIteration<? extends Statement, QueryEvaluationException> iteration = tripleSource
							.getStatements((Resource) bindingSet.getBinding(name).getValue(), expantionProperties[e],
									null)) {
						while (iteration.hasNext()) {
							Statement next = iteration.next();
							if (next.getObject().isLiteral()) {
								Literal literal = (Literal) next.getObject();
								if (literal.getLanguage().isPresent()) {
									for (int i = 0; i < languages.size(); i++) {
										if (literal.getLanguage().get().equals(languages.get(i))) {
											languageLabels[i] = literal.getLabel();
										}
									}
								}
							}
						}
					}
				}
				boolean found = false;
				for (String languageLabel : languageLabels) {
					if (languageLabel != null) {
						valuesWithLabels.add(vf.createLiteral(languageLabel));
						found = true;
						break;
					}

				}
				if (found == false) {
					valuesWithLabels.add(vf.createLiteral(""));
				}
			}
		}
		return new ListBindingSet(namesWithLabels, valuesWithLabels);
	}

	@Override
	public boolean isInitialized() {
		return false;
	}

	@Override
	public void initialize() throws QueryEvaluationException {

	}

	@Override
	public void shutdown() throws QueryEvaluationException {

	}
}
