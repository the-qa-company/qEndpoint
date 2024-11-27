package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.model.HDTValue;
import com.the_qa_company.qendpoint.model.SimpleIRIHDT;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.LookAheadIteration;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.impl.SimpleBinding;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class ProtoypeJoinIterator extends LookAheadIteration<BindingSet> {

	private final CloseableIteration<TripleID> leftIter;
	private final SimpleIRIHDT rightPredicate;
	private final HDTValue rightObject;
	private final HDTValue rightContext;
	private final EndpointTripleSource endpointTripleSource;
	private final StatementPattern left1;
	private final StatementPattern right1;

	private TripleID currentLeft = null;

	private CloseableIteration<TripleID> rightIter;
	private Set<String> name;
	private List<String> name1;

	public ProtoypeJoinIterator(CloseableIteration<TripleID> statements,
								SimpleIRIHDT rightPredicate, HDTValue rightObject, HDTValue rightContext,
								EndpointTripleSource endpointTripleSource,
								StatementPattern left, StatementPattern right) {
		leftIter = statements;
		this.rightPredicate = rightPredicate;
		this.rightObject = rightObject;
		this.rightContext = rightContext;

		this.endpointTripleSource = endpointTripleSource;

		left1 = left;
		right1 = right;

		String objectVarName = right1.getObjectVar().getName();
		if (right1.getObjectVar().isConstant()) {
			objectVarName = left1.getObjectVar().getName();
		}

		name = Set.of(left1.getSubjectVar().getName(), objectVarName);
		name1 = List.of(left1.getSubjectVar().getName(), objectVarName);
	}

	@Override
	protected BindingSet getNextElement() throws QueryEvaluationException {
		if (rightIter != null) {
			if (rightIter.hasNext()) {
				return join(currentLeft, rightIter.next());
			} else {
				rightIter.close();
			}
		}

		while (leftIter.hasNext()) {
			currentLeft = leftIter.next();
			rightIter = getIterator(currentLeft);
			if (rightIter.hasNext()) {
				return join(currentLeft, rightIter.next());
			} else {
				rightIter.close();
			}
		}

		return null;
	}

	private BindingSet join(TripleID left, TripleID right) {

		long object;

		if (right1.getObjectVar().isConstant()) {
			object = left.getObject();
		} else {
			object = right.getObject();
		}

		PrototypeBindingSet bindings = new PrototypeBindingSet(name, name1, new long[]{left.getSubject(), object},
				endpointTripleSource.getEndpointStore().getHdtConverter());

		return bindings;
	}

	private static class PrototypeBindingSet implements BindingSet {

		private static final long serialVersionUID = 1858454487384051888L;

		private final Set<String> bindingNamesSet;
		private final long[] values;
		private final List<String> varNamesList;
		private final HDTConverter hdtConverter;

		private int cachedHashCode = 0;

		public PrototypeBindingSet(Set<String> bindingNamesSet, List<String> varNamesList, long[] values,
								   HDTConverter hdtConverter) {
			this.hdtConverter = hdtConverter;
			assert varNamesList.size() == values.length;
			assert !varNamesList.isEmpty();
			this.bindingNamesSet = bindingNamesSet;
			this.varNamesList = varNamesList;
			this.values = values;

		}

		@Override
		public Iterator<Binding> iterator() {
			ArrayList<Binding> bindings = new ArrayList<>(varNamesList.size());
			for (int i = 0; i < varNamesList.size(); i++) {
				String varName = varNamesList.get(i);
				Binding binding = getBinding(varName);
				if (binding != null) {
					bindings.add(binding);
				}
			}
			return bindings.iterator();
		}

		@Override
		public Set<String> getBindingNames() {
			return bindingNamesSet;
		}

		@Override
		public Binding getBinding(String bindingName) {
			String s = varNamesList.get(0);
			if (s.equals(bindingName)) {
				return new SimpleBinding(bindingName, hdtConverter.idToSubjectHDTResource(values[0]));
			}

			String s1 = varNamesList.get(1);
			if (s1.equals(bindingName)) {
				return new SimpleBinding(bindingName, hdtConverter.idToObjectHDTResource(values[1]));
			}

			return null;
		}

		@Override
		public boolean hasBinding(String bindingName) {
			return bindingNamesSet.contains(bindingName);
		}

		@Override
		public Value getValue(String bindingName) {
			Binding binding = getBinding(bindingName);
			if (binding != null) {
				return binding.getValue();
			}
			return null;
		}

		@Override
		public int size() {
			int size = 0;
			for (Long value : values) {
				if (value != 0) {
					size++;
				}
			}

			return size;
		}

		@Override
		public int hashCode() {
			if (cachedHashCode == 0) {
				int hashCode = 0;
				for (Binding binding : this) {
					hashCode ^= binding.getName().hashCode() ^ binding.getValue().hashCode();
				}
				cachedHashCode = hashCode;
			}
			return cachedHashCode;
		}

		@Override
		public boolean equals(Object other) {
			if (this == other) {
				return true;
			}

			if (!(other instanceof BindingSet)) {
				return false;
			}

			BindingSet that = (BindingSet) other;

			if (this.size() != that.size()) {
				return false;
			}

			if (this.size() == 1) {
				Binding binding = iterator().next();
				Binding thatBinding = that.iterator().next();

				return binding.getName().equals(thatBinding.getName())
						&& binding.getValue().equals(thatBinding.getValue());
			}

			// Compare other's bindings to own
			for (Binding binding : that) {
				Value ownValue = getValue(binding.getName());

				if (!binding.getValue().equals(ownValue)) {
					// Unequal bindings for this name
					return false;
				}
			}

			return true;
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder(32 * size());

			sb.append('[');

			Iterator<Binding> iter = iterator();
			while (iter.hasNext()) {
				sb.append(iter.next().toString());
				if (iter.hasNext()) {
					sb.append(';');
				}
			}

			sb.append(']');

			return sb.toString();
		}

		@Override
		public boolean isEmpty() {
			return false;
		}
	}

	private CloseableIteration<TripleID> getIterator(TripleID currentLeft) {
		return endpointTripleSource.prototypeGetStatements(null, currentLeft.getSubject(),
				rightPredicate != null ? rightPredicate.getHDTId() : 0,
				rightObject != null ? rightObject.getHDTId() : 0, rightContext != null ? rightContext.getHDTId() : 0);
	}

	@Override
	protected void handleClose() throws QueryEvaluationException {
		try {
			leftIter.close();
		} finally {
			if (rightIter != null) {
				rightIter.close();
			}
		}
	}
}
