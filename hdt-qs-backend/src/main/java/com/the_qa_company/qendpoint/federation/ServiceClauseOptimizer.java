package com.the_qa_company.qendpoint.federation;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractSimpleQueryModelVisitor;

/**
 * Service optimizer that moves the SERVICE clause for wikibase:label to the
 * right position
 */
public class ServiceClauseOptimizer implements QueryOptimizer {

	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		tupleExpr.visit(new ServiceVisitor());
	}

	/**
	 * changes the order a service clause in a block of joins and left joins
	 */
	private static class ServiceVisitor extends AbstractSimpleQueryModelVisitor<RuntimeException> {
		@Override
		public void meet(Service node) {
			// this optimization applies only for the wikibase language service
			if (node instanceof Service) {
				Value serviceUrl = node.getServiceRef().getValue();
				if (serviceUrl instanceof IRI) {
					if (serviceUrl.stringValue().equals("http://wikiba.se/ontology#label")) {
						// optimize only if the service clause is in a triple
						// pattern block
						if (node.getParentNode() instanceof Join || node.getParentNode() instanceof LeftJoin) {
							// find the latest join or optional in the block
							// where the service clause appears
							QueryModelNode root = findRoot(node);
							QueryModelNode parentNode = root.getParentNode();

							// move service node above root
							Join newRoot = null;
							if (root instanceof Join) {
								newRoot = new Join((Join) root, node.clone());
							}
							if (root instanceof LeftJoin) {
								newRoot = new Join((LeftJoin) root, node.clone());
							}
							parentNode.replaceChildNode(root, newRoot);

							// remove service node at the bottom
							if (node.getParentNode() instanceof Join) {
								Join leave = (Join) node.getParentNode().clone();
								if (leave.getLeftArg() instanceof Service) {
									node.getParentNode().replaceWith(leave.getRightArg().clone());
								} else {
									node.getParentNode().replaceWith(leave.getLeftArg().clone());
								}
							}
							if (node.getParentNode() instanceof LeftJoin) {
								LeftJoin leave = (LeftJoin) node.getParentNode();
								if (leave.getLeftArg() instanceof Service) {
									node.getParentNode().replaceWith(leave.getRightArg().clone());
								} else {
									node.getParentNode().replaceWith(leave.getLeftArg().clone());
								}
							}
						}
					}
					;
				}
			}
		}
	}

	/**
	 * @param node
	 * @return the last node in the block that is a JOIN of a LEFT JOIN
	 */
	private static QueryModelNode findRoot(QueryModelNode node) {
		if (node.getParentNode() instanceof Join || node.getParentNode() instanceof LeftJoin) {
			return findRoot(node.getParentNode());
		}
		return node;
	}

}
