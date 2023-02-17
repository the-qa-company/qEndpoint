package com.the_qa_company.qendpoint.utils;

import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.model.SimpleIRIHDT;
import com.the_qa_company.qendpoint.store.EndpointStore;
import com.the_qa_company.qendpoint.store.HDTConverter;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

/**
 * QueryOptimizer to replace Var by IRIHDT Var in a query
 *
 * @author Dennis Diefenbach
 */
public class VariableToIdSubstitution implements QueryOptimizer {

	private final HDTConverter converter;
	private final HDT hdt;

	/**
	 * create the optimizer
	 *
	 * @param store the store to get the hdt
	 */
	public VariableToIdSubstitution(EndpointStore store) {
		this.hdt = store.getHdt();
		this.converter = store.getHdtConverter();
	}

	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		Substituor substitutor = new Substituor();
		tupleExpr.visit(substitutor);
	}

	protected class Substituor extends AbstractQueryModelVisitor<RuntimeException> {

		@Override
		public void meet(Var var) {
			if (var.isAnonymous() && var.hasValue()) {
				String iriString = var.getValue().toString();
				long id = hdt.getDictionary().stringToId(iriString, TripleComponentRole.SUBJECT);
				int position;
				if (id != -1) {
					if (id <= hdt.getDictionary().getNshared()) {
						position = SimpleIRIHDT.SHARED_POS;
					} else {
						position = SimpleIRIHDT.SUBJECT_POS;
					}
				} else {
					id = hdt.getDictionary().stringToId(iriString, TripleComponentRole.OBJECT);
					if (id != -1) {
						position = SimpleIRIHDT.OBJECT_POS;
					} else {
						id = hdt.getDictionary().stringToId(iriString, TripleComponentRole.PREDICATE);
						position = SimpleIRIHDT.PREDICATE_POS;
					}
				}
				if (id != -1) {
					var.setValue(converter.idToHDTValue(id, position));
				}
			}
		}

		@Override
		public void meet(BindingSetAssignment bindings) {
			for (BindingSet binding : bindings.getBindingSets()) {
				binding.getBindingNames().iterator();
			}
		}
	}
}
