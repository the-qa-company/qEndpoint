package org.rdfhdt.hdt.rdf4j.misc;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.rdf4j.utility.HDTConverter;
import org.rdfhdt.hdt.triples.TripleID;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HDTEvaluationStatistics extends EvaluationStatistics {

  private final HDT hdt;

  HDTEvaluationStatistics(HDT hdt) {
    this.hdt = hdt;
  }

  @Override
  protected CardinalityCalculator createCardinalityCalculator() {
    return new HDTCardinalityCalculator();
  }

  protected class HDTCardinalityCalculator extends CardinalityCalculator {

    @Override
    public double getCardinality(StatementPattern sp) {
      Value subj = getConstantValue(sp.getSubjectVar());
      if (!(subj instanceof Resource)) {
        // can happen when a previous optimizer has inlined a comparison
        // operator.
        // this can cause, for example, the subject variable to be
        // equated to a literal value.
        // See SES-970 / SES-998
        subj = null;
      }
      Value pred = getConstantValue(sp.getPredicateVar());
      if (!(pred instanceof IRI)) {
        // can happen when a previous optimizer has inlined a comparison
        // operator. See SES-970 / SES-998
        pred = null;
      }
      Value obj = getConstantValue(sp.getObjectVar());
      Value context = getConstantValue(sp.getContextVar());
      if (!(context instanceof Resource)) {
        // can happen when a previous optimizer has inlined a comparison
        // operator. See SES-970 / SES-998
        context = null;
      }

      HDTConverter hdtConverter = new HDTConverter(hdt);
      long subject = hdtConverter.subjectId((Resource) subj);
      long predicate = hdtConverter.predicateId((IRI) pred);
      long object = hdtConverter.objectId(obj);

      // Perform look-ups for value-equivalents of the specified values
      //            MemResource memSubj = valueFactory.getMemResource((Resource) subj);
      //            MemIRI memPred = valueFactory.getMemURI((IRI) pred);
      //            MemValue memObj = valueFactory.getMemValue(obj);
      //            MemResource memContext = valueFactory.getMemResource((Resource) context);
      //
      if (subject == -1 || predicate == -1 || object == -1) {
        // non-existent subject, predicate, object or context
        return 0.0;
      }

      // Search for the smallest list that can be used by the iterator
      List<Integer> listSizes = new ArrayList<>(4);
      if (subject != -1 && subject != 0) {
        listSizes.add(
            (int) hdt.getTriples().search(new TripleID(subject, 0, 0)).estimatedNumResults());
      }
      if (predicate != -1) {
        listSizes.add(
            (int) hdt.getTriples().search(new TripleID(0, predicate, 0)).estimatedNumResults());
      }
      if (object != -1 && object != 0) {
        listSizes.add(
            (int) hdt.getTriples().search(new TripleID(0, 0, object)).estimatedNumResults());
      }
      //            if (memContext != null) {
      //                listSizes.add(memContext.getContextStatementCount());
      //            }
      //
      double cardinality;

      if (listSizes.isEmpty()) {
        // all wildcards
        cardinality = Integer.MAX_VALUE;
      } else {
        cardinality = (double) Collections.min(listSizes);

        // List<Var> vars = getVariables(sp);
        // int constantVarCount = countConstantVars(vars);
        //
        // // Subtract 1 from var count as this was used for the list
        // size
        // double unboundVarFactor = (double)(vars.size() -
        // constantVarCount) / (vars.size() - 1);
        //
        // cardinality = Math.pow(cardinality, unboundVarFactor);
      }

      return cardinality;
    }

    protected Value getConstantValue(Var var) {
      if (var != null) {
        return var.getValue();
      }

      return null;
    }
  }
}
