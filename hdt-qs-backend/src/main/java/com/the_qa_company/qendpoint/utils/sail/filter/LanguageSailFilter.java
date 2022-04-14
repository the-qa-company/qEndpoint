package com.the_qa_company.qendpoint.utils.sail.filter;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.sail.UpdateContext;

import java.util.Optional;

/**
 * Implementation of {@link SailFilter} to filter statements by literal languages
 *
 * @author Antoine Willerval
 */
public class LanguageSailFilter implements SailFilter {
	private String language;
	private boolean acceptNoLanguageLiterals;
	private boolean shouldHandleExpression;

	public LanguageSailFilter(String language, boolean acceptNoLanguageLiterals, boolean shouldHandleExpression) {
		this.language = language;
		this.acceptNoLanguageLiterals = acceptNoLanguageLiterals;
		this.shouldHandleExpression = shouldHandleExpression;
	}

	public LanguageSailFilter(String language, boolean acceptNoLanguageLiterals) {
		this(language, acceptNoLanguageLiterals, true);
	}

	private boolean handleValue(Value obj) {
		if (obj == null || !obj.isLiteral()) {
			return false;
		}

		Literal literal = (Literal) obj;

		Optional<String> lang = literal.getLanguage();

		if (lang.isPresent()) {
			return lang.get().equals(language);
		} else {
			return acceptNoLanguageLiterals;
		}
	}

	@Override
	public boolean shouldHandleNotifyRemove(Resource subj, IRI pred, Value obj, Resource... contexts) {
		return handleValue(obj);
	}

	@Override
	public boolean shouldHandleNotifyAdd(Resource subj, IRI pred, Value obj, Resource... contexts) {
		return handleValue(obj);
	}

	@Override
	public boolean shouldHandleAdd(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) {
		return handleValue(obj);
	}

	@Override
	public boolean shouldHandleRemove(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) {
		return handleValue(obj);
	}

	@Override
	public boolean shouldHandleGet(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) {
		return handleValue(obj);
	}

	@Override
	public boolean shouldHandleExpression(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred) {
		return shouldHandleExpression;
	}

	public String getLanguage() {
		return language;
	}

	public boolean isAcceptNoLanguageLiterals() {
		return acceptNoLanguageLiterals;
	}

	public boolean isShouldHandleExpression() {
		return shouldHandleExpression;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public void setAcceptNoLanguageLiterals(boolean acceptNoLanguageLiterals) {
		this.acceptNoLanguageLiterals = acceptNoLanguageLiterals;
	}

	public void setShouldHandleExpression(boolean shouldHandleExpression) {
		this.shouldHandleExpression = shouldHandleExpression;
	}
}
