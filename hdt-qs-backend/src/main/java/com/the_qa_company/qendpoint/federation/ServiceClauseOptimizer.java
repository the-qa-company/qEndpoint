//package com.the_qa_company.qendpoint.federation;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
//import org.eclipse.rdf4j.query.BindingSet;
//import org.eclipse.rdf4j.query.Dataset;
//import org.eclipse.rdf4j.query.algebra.AbstractQueryModelNode;
//import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
//import org.eclipse.rdf4j.query.algebra.Extension;
//import org.eclipse.rdf4j.query.algebra.Join;
//import org.eclipse.rdf4j.query.algebra.LeftJoin;
//import org.eclipse.rdf4j.query.algebra.StatementPattern;
//import org.eclipse.rdf4j.query.algebra.TupleExpr;
//import org.eclipse.rdf4j.query.algebra.Var;
//import org.eclipse.rdf4j.query.algebra.ZeroLengthPath;
//import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
//import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
//import org.eclipse.rdf4j.query.algebra.helpers.AbstractSimpleQueryModelVisitor;
//import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternVisitor;
//import org.eclipse.rdf4j.query.algebra.helpers.TupleExprs;
//
//public class ServiceClauseOptimizer implements QueryOptimizer {
//
//    private final boolean trackResultSize;
//
//    public ServiceClauseOptimizer() {}
//
//    public ServiceClauseOptimizer(boolean trackResultSize) {
//        this.trackResultSize = trackResultSize;
//    }
//
//    /**
//     * Applies generally applicable optimizations: path expressions are sorted from more to less specific.
//     *
//     * @param tupleExpr
//     */
//    @Override
//    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
//        tupleExpr.visit(new ServiceClauseOptimizer.JoinVisitor(trackResultSize));
//    }
//
//    private static class JoinVisitor extends AbstractSimpleQueryModelVisitor<RuntimeException> {
//
//        Set<String> boundVars = new HashSet<>();
//
//        protected JoinVisitor(boolean trackResultSize) {
//            super(trackResultSize);
//        }
//
//        @Override
//        public void meet(Join node) {
//
//            Set<String> origBoundVars = boundVars;
//            try {
//                boundVars = new HashSet<>(boundVars);
//
//                // Recursively get the join arguments
//                List<TupleExpr> joinArgs = getJoinArgs(node, new ArrayList<>());
//
//                // get all extensions (BIND clause)
//                List<TupleExpr> orderedExtensions = getExtensionTupleExprs(joinArgs);
//                joinArgs.removeAll(orderedExtensions);
//
//                // Reorder the (recursive) join arguments to a more optimal sequence
//                List<TupleExpr> orderedJoinArgs = new ArrayList<>(joinArgs.size());
//
//                // We order all remaining join arguments based on cardinality and
//                // variable frequency statistics
//                if (joinArgs.size() > 0) {
//                    // Build maps of cardinalities and vars per tuple expression
//                    Map<TupleExpr, Double> cardinalityMap = Collections.emptyMap();
//                    Map<TupleExpr, List<Var>> varsMap = new HashMap<>();
//
//                    for (TupleExpr tupleExpr : joinArgs) {
//                        if (tupleExpr instanceof Join) {
//                            // we can skip calculating the cardinality for instances of Join since we will anyway "meet"
//                            // these nodes
//                            continue;
//                        }
//
//                        double cardinality = statistics.getCardinality(tupleExpr);
//
//                        tupleExpr.setResultSizeEstimate(Math.max(cardinality, tupleExpr.getResultSizeEstimate()));
//                        if (!hasCachedCardinality(tupleExpr)) {
//                            if (cardinalityMap.isEmpty()) {
//                                cardinalityMap = new HashMap<>();
//                            }
//                            cardinalityMap.put(tupleExpr, cardinality);
//                        }
//                        if (tupleExpr instanceof ZeroLengthPath) {
//                            varsMap.put(tupleExpr, ((ZeroLengthPath) tupleExpr).getVarList());
//                        } else {
//                            varsMap.put(tupleExpr, getStatementPatternVars(tupleExpr));
//                        }
//                    }
//
//                    // Build map of var frequences
//                    Map<Var, Integer> varFreqMap = new HashMap<>((varsMap.size() + 1) * 2);
//                    for (List<Var> varList : varsMap.values()) {
//                        fillVarFreqMap(varList, varFreqMap);
//                    }
//
//                    // order all other join arguments based on available statistics
//                    while (!joinArgs.isEmpty()) {
//                        TupleExpr tupleExpr = selectNextTupleExpr(joinArgs, cardinalityMap, varsMap, varFreqMap);
//
//                        joinArgs.remove(tupleExpr);
//                        orderedJoinArgs.add(tupleExpr);
//
//                        // Recursively optimize join arguments
//                        tupleExpr.visit(this);
//
//                        boundVars.addAll(tupleExpr.getBindingNames());
//                    }
//                }
//
//                // Build new join hierarchy
//                TupleExpr priorityJoins = null;
//                if (priorityArgs.size() > 0) {
//                    priorityJoins = priorityArgs.get(0);
//                    for (int i = 1; i < priorityArgs.size(); i++) {
//                        priorityJoins = new Join(priorityJoins, priorityArgs.get(i));
//                    }
//                }
//
//                if (orderedJoinArgs.size() > 0) {
//                    // Note: generated hierarchy is right-recursive to help the
//                    // IterativeEvaluationOptimizer to factor out the left-most join
//                    // argument
//                    int i = orderedJoinArgs.size() - 1;
//                    TupleExpr replacement = orderedJoinArgs.get(i);
//                    for (i--; i >= 0; i--) {
//                        replacement = new Join(orderedJoinArgs.get(i), replacement);
//                    }
//
//                    if (priorityJoins != null) {
//                        replacement = new Join(priorityJoins, replacement);
//                    }
//
//                    // Replace old join hierarchy
//                    node.replaceWith(replacement);
//
//                    // we optimize after the replacement call above in case the optimize call below
//                    // recurses back into this function and we need all the node's parent/child pointers
//                    // set up correctly for replacement to work on subsequent calls
//                    if (priorityJoins != null) {
//                        optimizePriorityJoin(origBoundVars, priorityJoins);
//                    }
//
//                } else {
//                    // only subselect/priority joins involved in this query.
//                    node.replaceWith(priorityJoins);
//                }
//            } finally {
//                boundVars = origBoundVars;
//            }
//        }
//
//        protected <L extends List<TupleExpr>> L getJoinArgs(TupleExpr tupleExpr, L joinArgs) {
//            if (tupleExpr instanceof Join) {
//                Join join = (Join) tupleExpr;
//                getJoinArgs(join.getLeftArg(), joinArgs);
//                getJoinArgs(join.getRightArg(), joinArgs);
//            } else {
//                joinArgs.add(tupleExpr);
//            }
//
//            return joinArgs;
//        }
//
//
//
//
//        /**
//         * Selects from a list of tuple expressions the next tuple expression that should be evaluated. This method
//         * selects the tuple expression with highest number of bound variables, preferring variables that have been
//         * bound in other tuple expressions over variables with a fixed value.
//         */
//        protected TupleExpr selectNextTupleExpr(List<TupleExpr> expressions, Map<TupleExpr, Double> cardinalityMap,
//                                                Map<TupleExpr, List<Var>> varsMap, Map<Var, Integer> varFreqMap) {
//            if (expressions.size() == 1) {
//                TupleExpr tupleExpr = expressions.get(0);
//                if (tupleExpr.getCostEstimate() < 0) {
//                    tupleExpr.setCostEstimate(getTupleExprCost(tupleExpr, cardinalityMap, varsMap, varFreqMap));
//                }
//                return tupleExpr;
//            }
//
//            TupleExpr result = null;
//            double lowestCost = Double.POSITIVE_INFINITY;
//
//            for (TupleExpr tupleExpr : expressions) {
//                // Calculate a score for this tuple expression
//                double cost = getTupleExprCost(tupleExpr, cardinalityMap, varsMap, varFreqMap);
//
//                if (cost < lowestCost || result == null) {
//                    // More specific path expression found
//                    lowestCost = cost;
//                    result = tupleExpr;
//                    if (cost == 0)
//                        break;
//                }
//            }
//
//            assert result != null;
//            result.setCostEstimate(lowestCost);
//
//            return result;
//        }
//
//        protected double getTupleExprCost(TupleExpr tupleExpr, Map<TupleExpr, Double> cardinalityMap,
//                                          Map<TupleExpr, List<Var>> varsMap, Map<Var, Integer> varFreqMap) {
//
//            // BindingSetAssignment has a typical constant cost. This cost is not based on statistics so is much more
//            // reliable. If the BindingSetAssignment binds to any of the other variables in the other tuple expressions
//            // to choose from, then the cost of the BindingSetAssignment should be set to 0 since it will always limit
//            // the upper bound of any other costs. This way the BindingSetAssignment will be chosen as the left
//            // argument.
//            if (tupleExpr instanceof BindingSetAssignment) {
//
//                Set<Var> varsUsedInOtherExpressions = varFreqMap.keySet();
//
//                for (String assuredBindingName : tupleExpr.getAssuredBindingNames()) {
//                    if (varsUsedInOtherExpressions.contains(new Var(assuredBindingName))) {
//                        return 0;
//                    }
//                }
//            }
//
//            double cost;
//
//            if (hasCachedCardinality(tupleExpr)) {
//                cost = ((AbstractQueryModelNode) tupleExpr).getCardinality();
//            } else {
//                cost = cardinalityMap.get(tupleExpr);
//            }
//
//            List<Var> vars = varsMap.get(tupleExpr);
//
//            // Compensate for variables that are bound earlier in the evaluation
//            List<Var> unboundVars = getUnboundVars(vars);
//            int constantVars = countConstantVars(vars);
//
//            int nonConstantVarCount = vars.size() - constantVars;
//
//            if (nonConstantVarCount > 0) {
//                double exp = (double) unboundVars.size() / nonConstantVarCount;
//                cost = Math.pow(cost, exp);
//            }
//
//            if (unboundVars.isEmpty()) {
//                // Prefer patterns with more bound vars
//                if (nonConstantVarCount > 0) {
//                    cost /= nonConstantVarCount;
//                }
//            } else {
//                // Prefer patterns that bind variables from other tuple expressions
//                int foreignVarFreq = getForeignVarFreq(unboundVars, varFreqMap);
//                if (foreignVarFreq > 0) {
//                    cost /= 1 + foreignVarFreq;
//                }
//            }
//
//            return cost;
//        }
//
//        private int countConstantVars(List<Var> vars) {
//            int size = 0;
//
//            for (Var var : vars) {
//                if (var.hasValue()) {
//                    size++;
//                }
//            }
//
//            return size;
//        }
//
//        @Deprecated(forRemoval = true, since = "4.1.0")
//        protected List<Var> getUnboundVars(Iterable<Var> vars) {
//
//            List<Var> ret = null;
//
//            for (Var var : vars) {
//                if (!var.hasValue() && var.getName() != null && !boundVars.contains(var.getName())) {
//                    if (ret == null) {
//                        ret = Collections.singletonList(var);
//                    } else {
//                        if (ret.size() == 1) {
//                            ret = new ArrayList<>(ret);
//                        }
//                        ret.add(var);
//                    }
//                }
//            }
//
//            return ret != null ? ret : Collections.emptyList();
//        }
//
//        protected List<Var> getUnboundVars(List<Var> vars) {
//            int size = vars.size();
//            if (size == 0) {
//                return List.of();
//            }
//            if (size == 1) {
//                Var var = vars.get(0);
//                if (!var.hasValue() && var.getName() != null && !boundVars.contains(var.getName())) {
//                    return List.of(var);
//                } else {
//                    return List.of();
//                }
//            }
//
//            List<Var> ret = null;
//
//            for (Var var : vars) {
//                if (!var.hasValue() && var.getName() != null && !boundVars.contains(var.getName())) {
//                    if (ret == null) {
//                        ret = List.of(var);
//                    } else {
//                        if (ret.size() == 1) {
//                            ret = new ArrayList<>(ret);
//                        }
//                        ret.add(var);
//                    }
//                }
//            }
//
//            return ret != null ? ret : Collections.emptyList();
//        }
//
//        protected int getForeignVarFreq(List<Var> ownUnboundVars, Map<Var, Integer> varFreqMap) {
//            if (ownUnboundVars.isEmpty()) {
//                return 0;
//            }
//            if (ownUnboundVars.size() == 1) {
//                return varFreqMap.get(ownUnboundVars.get(0)) - 1;
//            } else {
//                int result = -ownUnboundVars.size();
//                for (Var var : new HashSet<>(ownUnboundVars)) {
//                    result += varFreqMap.get(var);
//                }
//                return result;
//
//            }
//        }
//
//        private static class StatementPatternVarCollector extends StatementPatternVisitor {
//
//            private final TupleExpr tupleExpr;
//            private List<Var> vars;
//
//            public StatementPatternVarCollector(TupleExpr tupleExpr) {
//                this.tupleExpr = tupleExpr;
//            }
//
//            @Override
//            protected void accept(StatementPattern node) {
//                if (vars == null) {
//                    vars = new ArrayList<>(node.getVarList());
//                } else {
//                    vars.addAll(node.getVarList());
//                }
//            }
//
//            public List<Var> getVars() {
//                if (vars == null) {
//                    try {
//                        tupleExpr.visit(this);
//                    } catch (Exception e) {
//                        if (e instanceof InterruptedException) {
//                            Thread.currentThread().interrupt();
//                        }
//                        throw new IllegalStateException(e);
//                    }
//                    if (vars == null) {
//                        vars = Collections.emptyList();
//                    }
//                }
//
//                return vars;
//            }
//        }
//
//    }
//
//    private static int getJoinSize(Set<String> currentListNames, Set<String> names) {
//        int count = 0;
//        for (String name : names) {
//            if (currentListNames.contains(name)) {
//                count++;
//            }
//        }
//        return count;
//    }
//
//    private static boolean hasCachedCardinality(TupleExpr tupleExpr) {
//        return tupleExpr instanceof AbstractQueryModelNode
//                && ((AbstractQueryModelNode) tupleExpr).isCardinalitySet();
//    }
//
//}
//
