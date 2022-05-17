package com.the_qa_company.qendpoint.utils.sail.filter;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.sail.UpdateContext;
import org.eclipse.rdf4j.sail.lucene.LuceneSailSchema;

/**
 * a filter to find search:matches object of Lucene
 *
 * @author Antoine Willerval
 */
public class LuceneMatchExprSailFilter implements SailFilter {
    @Override
    public boolean shouldHandleAdd(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) {
        return true;
    }

    @Override
    public boolean shouldHandleRemove(UpdateContext op, Resource subj, IRI pred, Value obj, Resource... contexts) {
        return true;
    }

    @Override
    public boolean shouldHandleNotifyAdd(Resource subj, IRI pred, Value obj, Resource... contexts) {
        return true;
    }

    @Override
    public boolean shouldHandleNotifyRemove(Resource subj, IRI pred, Value obj, Resource... contexts) {
        return true;
    }

    @Override
    public boolean shouldHandleGet(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) {
        return true;
    }

    @Override
    public boolean shouldHandleExpression(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings,
            boolean includeInferred) {
        LuceneMatchExprQueryVisitor visitor = new LuceneMatchExprQueryVisitor();
        tupleExpr.visit(visitor);
        return visitor.found();
    }

    /**
     * class to search for a search:match of Lucene in an expression
     *
     * @author Antoine Willerval
     */
    public static class LuceneMatchExprQueryVisitor extends AbstractQueryModelVisitor<RuntimeException> {
        private boolean find;

        @Override
        public void meet(StatementPattern node) {
            // ignore this node if we found the search:matches
            if (found()) {
                return;
            }
            Value predicate = node.getPredicateVar().getValue();
            if (LuceneSailSchema.MATCHES.equals(predicate)) {
                find = true;
            }
        }

        @Override
        protected void meetNode(QueryModelNode node) {
            // ignore the next child if we found the search:matches
            if (!found()) {
                super.meetNode(node);
            }
        }

        /**
         * @return if we found the search:matches predicate
         */
        public boolean found() {
            return find;
        }
    }
}
