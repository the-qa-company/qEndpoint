package org.rdfhdt.hdt.rdf4j.utility;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.LookAheadIteration;
import org.eclipse.rdf4j.model.impl.SimpleIRIHDT;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class HDTJoinIterator extends LookAheadIteration<BindingSet, QueryEvaluationException> {
  ArrayList<IteratorPlusInt> list;
  private final EvaluationStrategy strategy;
  private final Join join;
  private final CloseableIteration<BindingSet, QueryEvaluationException> leftIter;
  private volatile CloseableIteration<BindingSet, QueryEvaluationException> rightIter;
  private boolean isMergeCase = false;

  public HDTJoinIterator(EvaluationStrategy strategy, Join join, BindingSet bindings)
      throws QueryEvaluationException {
    this.strategy = strategy;
    this.join = join;

    if (join.getLeftArg() instanceof StatementPattern
        && join.getRightArg() instanceof StatementPattern
        && bindings.size() == 0
        && ((StatementPattern) join.getLeftArg()).getSubjectVar().hasValue()
        && !((StatementPattern) join.getRightArg()).getSubjectVar().hasValue()
        && ((StatementPattern) join.getLeftArg())
            .getSubjectVar()
            .getName()
            .equals(((StatementPattern) join.getRightArg()).getSubjectVar().getName())) {
      System.out.println("Merging............................");
      // we resolve pattern 1 and pattern 2 with the bindings and then we do a merge join
      isMergeCase = true;
      this.leftIter = strategy.evaluate(join.getLeftArg(), bindings);
      this.rightIter = strategy.evaluate(join.getRightArg(), bindings);
      this.list = new ArrayList<>();
      if (leftIter.hasNext()) {
        list.add(new IteratorPlusInt(1, leftIter.next()));
      }
      if (rightIter.hasNext()) {
        list.add(new IteratorPlusInt(2, rightIter.next()));
      }
    } else {
      this.leftIter = strategy.evaluate(join.getLeftArg(), bindings);
      this.rightIter = new EmptyIteration();
    }
  }

  protected BindingSet getNextElement() throws QueryEvaluationException {
    BindingSet r = null;
    if (!isMergeCase) {
      while (this.rightIter.hasNext() || this.leftIter.hasNext()) {
        if (this.rightIter.hasNext()) {
          return (BindingSet) this.rightIter.next();
        }

        this.rightIter.close();
        if (this.leftIter.hasNext()) {
          this.rightIter =
              this.strategy.evaluate(this.join.getRightArg(), (BindingSet) this.leftIter.next());
        }
      }
      return r;
    } else {
      Collections.sort(list, new ScoreComparator());
      if (list.size() > 0) {
        if (list.get(0).getId() == list.get(1).getId()) {
          r = list.get(0).value;
          boolean remove = false;
          if (leftIter.hasNext()) {
            list.set(0, new IteratorPlusInt(1, leftIter.next()));
          } else {
            list.remove(0);
            remove = true;
          }
          if (rightIter.hasNext()) {
            if (remove == true) {
              list.set(0, new IteratorPlusInt(2, rightIter.next()));
            } else {
              list.set(1, new IteratorPlusInt(2, rightIter.next()));
            }

          } else {
            list.remove(0);
          }
        } else {

          if (list.get(0).iterator == 1) {
            r = list.get(0).value;
            if (leftIter.hasNext()) {
              list.set(0, new IteratorPlusInt(1, leftIter.next()));
            } else {
              list.remove(0);
            }
          } else {
            r = list.get(0).value;
            if (rightIter.hasNext()) {
              list.set(0, new IteratorPlusInt(2, rightIter.next()));
            } else {
              list.remove(0);
            }
          }
        }
      }
      return r;
    }
  }

  protected void handleClose() throws QueryEvaluationException {
    try {
      super.handleClose();
    } finally {
      try {
        this.leftIter.close();
      } finally {
        this.rightIter.close();
      }
    }
  }

  public class IteratorPlusInt {
    public int iterator;
    public BindingSet value;

    public IteratorPlusInt(int iterator, BindingSet value) {
      this.iterator = iterator;
      this.value = value;
    }

    public long getId() {
      if (iterator == 1) {
        // get the id of the variable from the left argument
        String hdt_Id =
            ((SimpleIRIHDT)
                    value
                        .getBinding(
                            ((StatementPattern) join.getLeftArg()).getSubjectVar().getName())
                        .getValue())
                .getHdtId();
        String id = hdt_Id.substring(hdt_Id.lastIndexOf("S") + 1, hdt_Id.length());
        return Long.parseLong(id);
      } else {
        String hdt_Id =
            ((SimpleIRIHDT)
                    value
                        .getBinding(
                            ((StatementPattern) join.getRightArg()).getSubjectVar().getName())
                        .getValue())
                .getHdtId();
        String id = hdt_Id.substring(hdt_Id.lastIndexOf("S") + 1, hdt_Id.length());
        return Long.parseLong(id);
      }
    }
  }

  public class ScoreComparator implements Comparator<IteratorPlusInt> {

    public int compare(IteratorPlusInt a, IteratorPlusInt b) {

      // long subjectId_a = 0;
      long subjectId_a = a.getId();
      long subjectId_b = b.getId();

      if (subjectId_a > subjectId_b) {
        return 1;
      } else if (subjectId_a < subjectId_b) {
        return -1;
      } else {
        return 0;
      }
    }
  }
}
