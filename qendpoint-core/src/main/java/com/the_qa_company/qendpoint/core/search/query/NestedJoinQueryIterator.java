package com.the_qa_company.qendpoint.core.search.query;

import com.the_qa_company.qendpoint.core.enums.DictionarySectionRole;
import com.the_qa_company.qendpoint.core.iterator.utils.FetcherIterator;
import com.the_qa_company.qendpoint.core.search.HDTQuery;
import com.the_qa_company.qendpoint.core.search.HDTQueryResult;
import com.the_qa_company.qendpoint.core.search.HDTQueryTool;
import com.the_qa_company.qendpoint.core.search.component.HDTComponent;
import com.the_qa_company.qendpoint.core.search.component.HDTComponentTriple;
import com.the_qa_company.qendpoint.core.search.component.HDTVariable;
import com.the_qa_company.qendpoint.core.search.component.SimpleHDTComponentTriple;
import com.the_qa_company.qendpoint.core.search.component.SimpleHDTConstant;
import com.the_qa_company.qendpoint.core.search.exception.HDTSearchTimeoutException;
import com.the_qa_company.qendpoint.core.search.result.MapHDTQueryResult;
import com.the_qa_company.qendpoint.core.triples.TripleID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class NestedJoinQueryIterator extends FetcherIterator<HDTQueryResult> {
	private final HDTQueryTool tool;
	private final HDTComponentTriple[] patterns;
	private final List<Iterator<TripleID>> iterators;
	private final MapHDTQueryResult result = new MapHDTQueryResult();
	private final long timeout;

	public NestedJoinQueryIterator(HDTQueryTool tool, HDTQuery query, long timeout) {
		this.tool = tool;
		if (timeout == 0) {
			this.timeout = Long.MAX_VALUE;
		} else {
			long current = System.currentTimeMillis();
			if (Long.MAX_VALUE - timeout > current) {
				this.timeout = current + timeout;
			} else {
				// too big, act like the user is asking an infinite timeout
				this.timeout = Long.MAX_VALUE;
			}
		}
		// mapping variable -> id
		Map<HDTVariable, WipHDTVariable> idMapping = new HashMap<>();

		Map<String, WipHDTVariable> tripleComponent = new HashMap<>(4);
		List<HDTComponentTriple> updatedPatterns = new ArrayList<>();
		for (HDTComponentTriple pattern : query.getPatterns()) {
			HDTComponent sp = pattern.getSubject();
			HDTComponent pp = pattern.getPredicate();
			HDTComponent op = pattern.getObject();

			tripleComponent.clear();

			HDTComponent s;
			HDTComponent p;
			HDTComponent o;
			if (sp.isVariable()) {
				HDTVariable variable = sp.asVariable();
				WipHDTVariable ivc = idMapping.get(variable);
				if (ivc == null) {
					WipHDTVariable iv = new WipHDTVariable(variable);
					result.set(variable.getName(), iv.constant);
					tripleComponent.put(variable.getName(), iv);
					idMapping.put(variable, iv);
					s = iv;
				} else {
					// replace by this constant, we can't update this variable
					// because a previous value is
					// already defining it
					s = ivc.constant;
				}
			} else {
				s = sp;
			}
			if (pp.isVariable()) {
				HDTVariable variable = pp.asVariable();
				WipHDTVariable dupeV = tripleComponent.get(variable.getName());
				if (dupeV != null) {
					p = dupeV;
				} else {
					WipHDTVariable ivc = idMapping.get(variable);
					if (ivc == null) {
						WipHDTVariable iv = new WipHDTVariable(variable);
						result.set(variable.getName(), iv.constant);
						tripleComponent.put(variable.getName(), iv);
						idMapping.put(variable, iv);
						p = iv;
					} else {
						// replace by this constant, we can't update this
						// variable
						// because a previous value is
						// already defining it
						p = ivc.constant;
					}
				}
			} else {
				p = pp;
			}
			if (op.isVariable()) {
				HDTVariable variable = op.asVariable();
				WipHDTVariable dupeV = tripleComponent.get(variable.getName());
				if (dupeV != null) {
					o = dupeV;
				} else {
					WipHDTVariable ivc = idMapping.get(variable);
					if (ivc == null) {
						WipHDTVariable iv = new WipHDTVariable(variable);
						result.set(variable.getName(), iv.constant);
						tripleComponent.put(variable.getName(), iv);
						idMapping.put(variable, iv);
						o = iv;
					} else {
						// replace by this constant, we can't update this
						// variable
						// because a previous value is
						// already defining it
						o = ivc.constant;
					}
				}
			} else {
				o = op;
			}
			updatedPatterns.add(new SimpleHDTComponentTriple(s, p, o));
		}

		this.patterns = updatedPatterns.toArray(new HDTComponentTriple[0]);
		iterators = new ArrayList<>(this.patterns.length);
		iterators.add(null);
		getOrCreatePattern(0);
		for (int i = 1; i < this.patterns.length; i++) {
			iterators.add(null);
		}
	}

	/**
	 * @return find first null iterator
	 */
	private int findFirstIterator() {
		for (int i = 1; i < iterators.size(); i++) {
			if (iterators.get(i) == null) {
				return i;
			}
		}
		return iterators.size();
	}

	private void checkTimeout() {
		if (System.currentTimeMillis() > timeout) {
			throw new HDTSearchTimeoutException();
		}
	}

	@Override
	protected HDTQueryResult getNext() {
		itSearch:
		while (iterators.get(0) != null) {
			int deep = findFirstIterator() - 1;

			for (int i = deep; i < iterators.size(); i++) {
				Iterator<TripleID> it = getOrCreatePattern(i);
				if (!it.hasNext()) {
					iterators.set(i, null);
					continue itSearch;
				}

				TripleID tid = it.next();

				HDTComponentTriple triple = patterns[i];

				HDTComponent s = triple.getSubject();
				HDTComponent p = triple.getPredicate();
				HDTComponent o = triple.getObject();

				// set the sub pattern variable
				if (s.isVariable()) {
					((WipHDTVariable) s).constant.setId(DictionarySectionRole.SUBJECT, tid.getSubject());
				}
				if (p.isVariable()) {
					((WipHDTVariable) p).constant.setId(DictionarySectionRole.PREDICATE, tid.getPredicate());
				}
				if (o.isVariable()) {
					((WipHDTVariable) o).constant.setId(DictionarySectionRole.OBJECT, tid.getObject());
				}
			}
			checkTimeout();
			return result;
		}
		return null;
	}

	/**
	 * get or create a pattern iterator from a pattern id
	 *
	 * @param id id
	 * @return iterator
	 */
	private Iterator<TripleID> getOrCreatePattern(int id) {
		if (iterators.get(id) != null) {
			return iterators.get(id);
		}
		Iterator<TripleID> it = tool.search(patterns[id]);
		iterators.set(id, it);
		return it;
	}

	private class WipHDTVariable implements HDTVariable {
		final HDTVariable original;
		final SimpleHDTConstant constant = new SimpleHDTConstant(tool.getHDT(), "");

		private WipHDTVariable(HDTVariable original) {
			this.original = original;
		}

		@Override
		public String stringValue() {
			return original.stringValue();
		}

		@Override
		public String getName() {
			return original.getName();
		}
	}
}
