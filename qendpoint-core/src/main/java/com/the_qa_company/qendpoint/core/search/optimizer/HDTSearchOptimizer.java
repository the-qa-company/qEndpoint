package com.the_qa_company.qendpoint.core.search.optimizer;

import com.the_qa_company.qendpoint.core.enums.DictionarySectionRole;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.search.component.HDTComponent;
import com.the_qa_company.qendpoint.core.search.component.HDTComponentTriple;
import com.the_qa_company.qendpoint.core.search.component.HDTComponentTripleWrapper;
import com.the_qa_company.qendpoint.core.search.component.HDTConstant;
import com.the_qa_company.qendpoint.core.search.component.HDTVariable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.OptionalInt;

public class HDTSearchOptimizer implements Optimizer {
	private static final HDTSearchOptimizer instance = new HDTSearchOptimizer();

	public static Optimizer getInstance() {
		return instance;
	}

	private static <T> OptionalInt min(List<T> list, Comparator<T> comparator) {
		if (list.isEmpty()) {
			return OptionalInt.empty();
		}
		int minIndex = 0;
		T min = list.get(0);

		for (int i = 1; i < list.size(); i++) {
			if (comparator.compare(min, list.get(i)) > 0) {
				min = list.get(i);
				minIndex = i;
			}
		}

		return OptionalInt.of(minIndex);
	}

	@Override
	public void optimize(List<HDTComponentTriple> patterns) {
		List<HDTComponentTripleMask> patternsTemp = new ArrayList<>(patterns.size());
		patterns.stream().map(HDTComponentTripleMask::new).forEach(patternsTemp::add);
		patterns.clear();

		while (!patternsTemp.isEmpty()) {
			HDTComponentTripleMask mask = patternsTemp.remove(min(patternsTemp, this::compareMask).orElseThrow());
			for (HDTVariable var : mask.vars()) {
				for (HDTComponentTripleMask tripleMask : patternsTemp) {
					tripleMask.replaceVar(var);
				}
			}
			patterns.add(mask.getHandle());
		}
	}

	private int patternPriority(HDTComponentTriple triple) {
		switch (triple.pattern()) {
		case "???":
		case "s??":
		case "sp?":
		case "spo":
			return 1;
		case "??o":
		case "?po":
			return 2;
		case "?p?":
			return 3;
		case "s?o":
			return 4;
		default:
			// hc
			return 5;
		}
	}

	private int varSize(HDTComponentTripleMask mask) {
		HDTComponent object = mask.getObject();
		HDTComponent predicate = mask.getPredicate();
		HDTComponent subject = mask.getSubject();

		// faster than creating a mutable list
		if (subject != null && subject.isVariable()) {
			if (predicate != null && predicate.isVariable()) {
				if (object != null && object.isVariable()) {
					return 3; // "???";
				} else {
					return 2; // "??o";
				}
			} else {
				if (object != null && object.isVariable()) {
					return 2; // "?p?";
				} else {
					return 1; // "?po";
				}
			}
		} else {
			if (predicate != null && predicate.isVariable()) {
				if (object != null && object.isVariable()) {
					return 2; // "s??";
				} else {
					return 2; // "s?o"; // equivalent to s??
				}
			} else {
				if (object != null && object.isVariable()) {
					return 1; // "sp?";
				} else {
					return 0; // "spo";
				}
			}
		}
	}

	private int compareMask(HDTComponentTripleMask o1, HDTComponentTripleMask o2) {
		int c = Integer.compare(varSize(o1), varSize(o2));
		if (c != 0) {
			return c;
		}

		return Integer.compare(patternPriority(o1), patternPriority(o2));
	}

	private static class HDTComponentTripleMask extends HDTComponentTripleWrapper {
		private HDTComponent subject;
		private HDTComponent predicate;
		private HDTComponent object;

		public HDTComponentTripleMask(HDTComponentTriple handle) {
			super(handle);
		}

		@Override
		public HDTComponent getSubject() {
			if (subject != null) {
				return subject;
			}
			return super.getSubject();
		}

		@Override
		public HDTComponent getPredicate() {
			if (predicate != null) {
				return predicate;
			}
			return super.getPredicate();
		}

		@Override
		public HDTComponent getObject() {
			if (object != null) {
				return object;
			}
			return super.getObject();
		}

		public void replaceVar(HDTVariable variable) {
			if (subject == null && super.getSubject().isVariable()
					&& super.getSubject().asVariable().getName().equals(variable.getName())) {
				subject = new EmptyConstant(variable);
			}
			if (predicate == null && super.getPredicate().isVariable()
					&& super.getPredicate().asVariable().getName().equals(variable.getName())) {
				predicate = new EmptyConstant(variable);
			}
			if (object == null && super.getObject().isVariable()
					&& super.getObject().asVariable().getName().equals(variable.getName())) {
				object = new EmptyConstant(variable);
			}
		}
	}

	private static class EmptyConstant implements HDTConstant {
		private final HDTVariable parent;

		private EmptyConstant(HDTVariable parent) {
			this.parent = parent;
		}

		@Override
		public String stringValue() {
			throw new NotImplementedException();
		}

		@Override
		public CharSequence getValue() {
			throw new NotImplementedException();
		}

		@Override
		public long getId(DictionarySectionRole role) {
			throw new NotImplementedException();
		}

		@Override
		public String toString() {
			return "<" + parent + ">";
		}
	}

}
