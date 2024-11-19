package com.the_qa_company.qendpoint.utils;

import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.model.SimpleIRIHDT;
import com.the_qa_company.qendpoint.store.EndpointStore;
import com.the_qa_company.qendpoint.store.HDTConverter;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
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
		public void meet(StatementPattern node) throws RuntimeException {
			Var subjectVar = node.getSubjectVar();
			if (subjectVar != null && subjectVar.isAnonymous() && subjectVar.hasValue()) {
				long id = converter.subjectToID(((Resource) subjectVar.getValue()));
				if (id != -1) {
					Var var1 = new Var(subjectVar.getName(), converter.idToSubjectHDTResource(id), true,
							subjectVar.isConstant());
					node.replaceChildNode(subjectVar, var1);
				}
			}

			Var predicateVar = node.getPredicateVar();
			if (predicateVar != null && predicateVar.isAnonymous() && predicateVar.hasValue()) {
				long id = converter.predicateToID(((IRI) predicateVar.getValue()));
				if (id != -1) {
					Var var1 = new Var(predicateVar.getName(), converter.idToPredicateHDTResource(id), true,
							predicateVar.isConstant());
					node.replaceChildNode(predicateVar, var1);
				}
			}

			Var objectVar = node.getObjectVar();
			if (objectVar != null && objectVar.isAnonymous() && objectVar.hasValue()) {
				long id = converter.objectToID((objectVar.getValue()));
				if (id != -1) {
					Var var1 = new Var(objectVar.getName(), converter.idToObjectHDTResource(id), true,
							objectVar.isConstant());
					node.replaceChildNode(objectVar, var1);
				}
			}

// 		TripleComponentRole.GRAPH is not supported!
//
//			Var contextVar = node.getContextVar();
//			if (contextVar != null && contextVar.isAnonymous() && contextVar.hasValue()) {
//				long id = converter.contextToID((((Resource) contextVar.getValue())));
//				if (id != -1) {
//					Var var1 = new Var(contextVar.getName(), converter.idToGraphHDTResource(id), true,
//							contextVar.isConstant());
//					node.replaceChildNode(contextVar, var1);
//				}
//			}

		}

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
					Var var1 = new Var(var.getName(), converter.idToHDTValue(id, position), var.isAnonymous(),
							var.isConstant());
					var.replaceWith(var1);
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
