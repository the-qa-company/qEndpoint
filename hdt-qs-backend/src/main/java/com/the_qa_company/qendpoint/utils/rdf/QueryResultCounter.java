package com.the_qa_company.qendpoint.utils.rdf;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryResultHandlerException;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;

import java.util.List;

public class QueryResultCounter implements TupleQueryResultHandler {
    private final TupleQueryResultHandler handle;
    private long count;

    public QueryResultCounter(TupleQueryResultHandler handle) {
        this.handle = handle;
    }

    @Override
    public void handleBoolean(boolean b) throws QueryResultHandlerException {
        handle.handleBoolean(b);
    }

    @Override
    public void handleLinks(List<String> list) throws QueryResultHandlerException {
        handle.handleLinks(list);
    }

    @Override
    public void startQueryResult(List<String> list) throws TupleQueryResultHandlerException {
        handle.startQueryResult(list);
    }

    @Override
    public void endQueryResult() throws TupleQueryResultHandlerException {
        handle.endQueryResult();
    }

    @Override
    public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {
        count++;
        handle.handleSolution(bindingSet);
    }

    public long getCount() {
        return count;
    }
}
