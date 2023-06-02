package com.the_qa_company.qendpoint.store.experimental;

import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.storage.QEPComponent;
import com.the_qa_company.qendpoint.store.experimental.model.QEPCoreValue;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;

import java.io.IOException;

class QEPCoreEvaluationStatistics extends EvaluationStatistics {
	private final QEPSailStore core;

	public QEPCoreEvaluationStatistics(QEPSailStore core) {
		this.core = core;
	}

	@Override
	protected EvaluationStatistics.CardinalityCalculator createCardinalityCalculator() {
		return new QEPCoreCardinalityCalculator();
	}

	private QEPComponent convertFrom(Value value) {
		if (value instanceof QEPCoreValue qcv) {
			return qcv.component();
		}
		return core.getCore().createComponentByString(value.toString());
	}

	private double cardinality(Resource subj, IRI pred, Value obj) throws IOException {
		QEPComponent s;
		QEPComponent p;
		QEPComponent o;

		if (subj != null) {
			s = convertFrom(subj);
			if (!s.exists(TripleComponentRole.SUBJECT)) {
				return 0; // doesn't exist in the store
			}
		} else {
			s = null;
		}

		if (pred != null) {
			p = convertFrom(pred);
			if (!p.exists(TripleComponentRole.SUBJECT)) {
				return 0; // doesn't exist in the store
			}
		} else {
			p = null;
		}

		if (obj != null) {
			o = convertFrom(obj);
			if (!o.exists(TripleComponentRole.OBJECT)) {
				return 0; // doesn't exist in the store
			}
		} else {
			o = null;
		}

		return core.getCore().cardinality(s, p, o);
	}

	protected class QEPCoreCardinalityCalculator extends EvaluationStatistics.CardinalityCalculator {

		@Override
		protected double getCardinality(StatementPattern sp) {
			try {
				Value subj = getConstantValue(sp.getSubjectVar());
				if (!(subj instanceof Resource)) {
					subj = null;
				}
				Value pred = getConstantValue(sp.getPredicateVar());
				if (!(pred instanceof IRI)) {
					pred = null;
				}
				Value obj = getConstantValue(sp.getObjectVar());

				return cardinality((Resource) subj, (IRI) pred, obj);
			} catch (IOException e) {
				return super.getCardinality(sp);
			}
		}

		protected Value getConstantValue(Var var) {
			return var != null ? var.getValue() : null;
		}
	}
}