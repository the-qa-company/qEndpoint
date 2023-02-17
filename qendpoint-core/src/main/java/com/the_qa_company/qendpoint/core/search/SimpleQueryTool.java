package com.the_qa_company.qendpoint.core.search;

import com.the_qa_company.qendpoint.core.enums.DictionarySectionRole;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.search.component.HDTComponent;
import com.the_qa_company.qendpoint.core.search.component.HDTComponentTriple;
import com.the_qa_company.qendpoint.core.search.component.HDTConstant;
import com.the_qa_company.qendpoint.core.search.component.HDTVariable;
import com.the_qa_company.qendpoint.core.search.component.SimpleHDTComponentTriple;
import com.the_qa_company.qendpoint.core.search.component.SimpleHDTConstant;
import com.the_qa_company.qendpoint.core.search.component.SimpleHDTVariable;
import com.the_qa_company.qendpoint.core.search.component.VarPattern;
import com.the_qa_company.qendpoint.core.search.query.NestedJoinQueryIterator;
import com.the_qa_company.qendpoint.core.search.utils.iterator.DupeSearchIterator;
import com.the_qa_company.qendpoint.core.triples.TripleID;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * simple query tool, use basic join to find results
 */
public class SimpleQueryTool implements HDTQueryTool {
	private static final String AN_PREFIX = "an_" + (new Random().nextLong());
	private static final AtomicLong AN_SUFFIX = new AtomicLong();
	private final HDT hdt;
	private final Map<String, String> prefixes = new HashMap<>();

	public SimpleQueryTool(HDT hdt) {
		this.hdt = hdt;
	}

	@Override
	public HDTQuery createQuery(Collection<HDTComponentTriple> patterns) {
		return new SimpleQueryPattern(this, patterns);
	}

	@Override
	public HDTVariable variable() {
		return new SimpleHDTVariable(hdt, AN_PREFIX + AN_SUFFIX.incrementAndGet());
	}

	@Override
	public HDTVariable variable(String name) {
		if (name == null || name.isEmpty()) {
			return variable();
		}
		return new SimpleHDTVariable(hdt, name);
	}

	@Override
	public HDTConstant constant(CharSequence str) {
		return new SimpleHDTConstant(hdt, str);
	}

	@Override
	public HDTConstant constant(long id, DictionarySectionRole role) {
		return new SimpleHDTConstant(hdt, id, role);
	}

	@Override
	public HDTComponent component(String component) {
		if (component.isEmpty() || component.equals("[]")) {
			return variable();
		}
		if (component.charAt(0) == '?') {
			if (component.length() == 1) {
				return variable();
			} else {
				return variable(component.substring(1));
			}
		}
		if (component.charAt(0) == '"' || component.charAt(0) == '_') {
			// bnode, iri or literal, ignore
			return constant(component);
		}
		if (component.charAt(0) == '<') {
			if (component.charAt(component.length() - 1) != '>') {
				throw new IllegalArgumentException("Bad iri format: " + component);
			}
			return constant(component.substring(1, component.length() - 1));
		}

		int shift = component.indexOf(':');
		if (shift == -1) {
			throw new IllegalArgumentException("Unknown component type: " + component);
		}
		String prefix = component.substring(0, shift);
		String location = prefixes.get(prefix);
		if (location == null) {
			throw new IllegalArgumentException("Unknown prefix: " + prefix + " in " + component);
		}
		return constant(location + component.substring(shift + 1));
	}

	@Override
	public void registerPrefix(String prefix, String location) {
		prefixes.put(prefix, location);
	}

	@Override
	public void unregisterPrefix(String prefix) {
		prefixes.remove(prefix);
	}

	@Override
	public String getPrefix(String prefix) {
		return prefixes.get(prefix);
	}

	@Override
	public Set<String> getPrefixes() {
		return Collections.unmodifiableSet(prefixes.keySet());
	}

	@Override
	public HDTComponentTriple triple(HDTComponent s, HDTComponent p, HDTComponent o) {
		return new SimpleHDTComponentTriple(s, p, o);
	}

	@Override
	public HDT getHDT() {
		return hdt;
	}

	@Override
	public Iterator<HDTQueryResult> query(HDTQuery q) {
		return new NestedJoinQueryIterator(this, q, q.getTimeout());
	}

	/**
	 * search if at least 2 vars are equal
	 *
	 * @param pattern pattern
	 * @return triple id iterator
	 */
	protected Iterator<TripleID> dupeSearch(HDTComponentTriple pattern, VarPattern varPattern) {
		return new DupeSearchIterator(hdt, noDupeSearch(pattern), varPattern);
	}

	/**
	 * search if no vars are equal
	 *
	 * @param pattern pattern
	 * @return triple id iterator
	 */
	protected Iterator<TripleID> noDupeSearch(HDTComponentTriple pattern) {
		long s, p, o;
		if (pattern.getSubject() != null && pattern.getSubject().isConstant()) {
			s = pattern.getSubject().asConstant().getId(DictionarySectionRole.SUBJECT);
		} else {
			s = 0;
		}
		if (pattern.getPredicate() != null && pattern.getPredicate().isConstant()) {
			p = pattern.getPredicate().asConstant().getId(DictionarySectionRole.PREDICATE);
		} else {
			p = 0;
		}
		if (pattern.getObject() != null && pattern.getObject().isConstant()) {
			o = pattern.getObject().asConstant().getId(DictionarySectionRole.OBJECT);
		} else {
			o = 0;
		}
		return hdt.getTriples().search(new TripleID(s, p, o));
	}

	@Override
	public Iterator<TripleID> search(HDTComponentTriple pattern) {
		VarPattern varPattern = VarPattern.of(pattern);
		if (varPattern.hasDuplicated()) {
			return dupeSearch(pattern, varPattern);
		}
		return noDupeSearch(pattern);
	}
}
