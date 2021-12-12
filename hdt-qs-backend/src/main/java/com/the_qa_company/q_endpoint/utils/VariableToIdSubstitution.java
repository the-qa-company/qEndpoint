package com.the_qa_company.q_endpoint.utils;

import com.the_qa_company.q_endpoint.model.SimpleIRIHDT;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;

import java.util.Iterator;

public class VariableToIdSubstitution implements QueryOptimizer {

    private HDT hdt;
    private HDTDictionaryMapping hdtDictionaryMapping;

    public VariableToIdSubstitution(HDT hdt) {
        this.hdt = hdt;
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
                QueryModelNode parent = var.getParentNode();
                String iriString = var.getValue().toString();
                long id = hdt.getDictionary().stringToId(iriString, TripleComponentRole.SUBJECT);
                int position = -1;
                if (id != -1) {
                    if (id <= hdt.getDictionary().getNshared()) {
                        position = SimpleIRIHDT.SHARED_POS;
                    } else {
                        position = SimpleIRIHDT.SUBJECT_POS;
                    }
                } else {
                    id =
                            hdt.getDictionary().stringToId(iriString, TripleComponentRole.OBJECT);
                    if (id != -1) {
                        position = SimpleIRIHDT.OBJECT_POS;
                    } else {
                        id =
                                hdt.getDictionary()
                                        .stringToId(iriString, TripleComponentRole.PREDICATE);
                        if (id != -1) {
                            position = SimpleIRIHDT.PREDICATE_POS;
                        }
                    }
                }
                if (id != -1) {

                    var.setValue(new SimpleIRIHDT(hdt, position, id));
                }
            }
        }

        @Override
        public void meet(BindingSetAssignment bindings) {
            Iterator<BindingSet> bindingSetIterable = bindings.getBindingSets().iterator();
            while (bindingSetIterable.hasNext()) {
                BindingSet binding = bindingSetIterable.next();
                Iterator<String> bindingNamesIterator = binding.getBindingNames().iterator();
            }
        }
    }
}
