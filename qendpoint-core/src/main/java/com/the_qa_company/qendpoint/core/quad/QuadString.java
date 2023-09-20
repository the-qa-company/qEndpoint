package com.the_qa_company.qendpoint.core.quad;

import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.string.ByteString;

public class QuadString extends TripleString {
	protected CharSequence context;

	public QuadString() {
		super();
		context = "";
	}

	public QuadString(CharSequence subject, CharSequence predicate, CharSequence object, CharSequence context) {
		super(subject, predicate, object);
		this.context = context;
	}

	public QuadString(TripleString other) {
		super(other);
		this.context = other.getGraph();
	}

	@Override
	public void clear() {
		super.clear();
		context = "";
	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof QuadString qs)) {
			if (context.length() == 0) {
				// not a quad string, maybe it is a TripleString
				return super.equals(other);
			}
			return false;
		}
		return equalsCharSequence(subject, qs.subject) && equalsCharSequence(predicate, qs.predicate)
				&& equalsCharSequence(object, qs.object) && equalsCharSequence(context, qs.context);
	}

	@Override
	public CharSequence getGraph() {
		return context;
	}

	@Override
	public void setGraph(CharSequence context) {
		this.context = context;
	}

	@Override
	public void setAll(CharSequence subject, CharSequence predicate, CharSequence object) {
		setAll(subject, predicate, object, "");
	}

	/**
	 * Sets all components at once. Useful to reuse existing object instead of
	 * creating new ones for performance.
	 *
	 * @param subject   subject
	 * @param predicate predicate
	 * @param object    object
	 * @param context   context
	 */
	public void setAll(CharSequence subject, CharSequence predicate, CharSequence object, CharSequence context) {
		super.setAll(subject, predicate, object);
		this.context = context;
	}

	@Override
	public boolean match(TripleString pattern) {
		if (context.length() != 0
				&& !(pattern instanceof QuadString && equalsCharSequence(((QuadString) pattern).context, context))) {
			// if a context is defined, we don't match
			return false;
		}
		if (pattern.getSubject().length() == 0 || equalsCharSequence(pattern.getSubject(), this.subject)) {
			if (pattern.getPredicate().length() == 0 || equalsCharSequence(pattern.getPredicate(), this.predicate)) {
				return pattern.getObject().length() == 0 || equalsCharSequence(pattern.getObject(), this.object);
			}
		}
		return false;
	}

	@Override
	public boolean isEmpty() {
		return super.isEmpty() && context.length() == 0;
	}

	@Override
	public void read(String line) throws ParserException {
		super.read(line, true);
	}

	@Override
	public void read(String line, int start, int end) throws ParserException {
		super.read(line, start, end, true);
	}

	@Override
	public int hashCode() {
		// Same as Objects.hashCode(subject, predicate, object), with fewer
		// calls
		int s = subject == null ? 0 : subject.hashCode();
		int p = predicate == null ? 0 : predicate.hashCode();
		int o = object == null ? 0 : object.hashCode();
		int c = context == null ? 0 : context.hashCode();
		return 31 * (31 * (31 * (31 * s) + p) + o) + c;
	}

	@Override
	public QuadString tripleToString() {
		return new QuadString(subject.toString(), predicate.toString(), object.toString(), context.toString());
	}

	@Override
	public TripleString tripleToByteString() {
		return new QuadString(ByteString.copy(subject), ByteString.copy(predicate), ByteString.copy(object), ByteString.copy(context));
	}

	@Override
	public TripleString tripleToByteStringCast() {
		return new QuadString(ByteString.of(subject), ByteString.of(predicate), ByteString.of(object), ByteString.copy(context));
	}

	@Override
	public String toString() {
		if (context.length() == 0) {
			return super.toString();
		}
		return super.toString() + " " + context;
	}
}
