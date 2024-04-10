package com.the_qa_company.qendpoint.model;

import com.the_qa_company.qendpoint.store.HDTConverter;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;

import java.util.Objects;

public class HDTLazyStatement implements Statement {
	private final long s, p, o;
	private final HDTConverter converter;
	private Resource subject;
	private IRI predicate;
	private Value object;
	private Resource context;

	public HDTLazyStatement(long s, long p, long o, HDTConverter converter) {
		this.s = s;
		this.p = p;
		this.o = o;
		this.converter = converter;
	}

	@Override
	public Resource getSubject() {
		if (subject == null) {
			subject = converter.idToSubjectHDTResource(s);
		}
		return subject;
	}

	@Override
	public IRI getPredicate() {
		if (predicate == null) {
			predicate = converter.idToPredicateHDTResource(p);
		}
		return predicate;
	}

	@Override
	public Value getObject() {
		if (object == null) {
			object = converter.idToObjectHDTResource(o);
		}
		return object;
	}

	@Override
	public Resource getContext() {
		return context; // fixme: implement
	}

	public void setSubject(Resource subject) {
		this.subject = subject;
	}

	public void setPredicate(IRI predicate) {
		this.predicate = predicate;
	}

	public void setObject(Value object) {
		this.object = object;
	}

	public void setContext(Resource context) {
		this.context = context;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj instanceof HDTLazyStatement hls) {
			return hls.s == s && hls.p == p && hls.o == o;
		}
		if (!(obj instanceof Statement stmt)) {
			return false;
		}
		return Objects.equals(getPredicate(), stmt.getPredicate()) && Objects.equals(getSubject(), stmt.getSubject())
				&& Objects.equals(getObject(), stmt.getObject());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getSubject(), getPredicate(), getObject(), getContext());
	}
}
