package com.the_qa_company.q_endpoint.utils;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.sail.SailException;

/**
 * Combine two statement CloseableIterations into one CloseableIteration
 * @author Ali Haidar
 */
public class CombinedNativeStoreResult implements CloseableIteration<Statement, SailException> {

    private final CloseableIteration<? extends Statement, SailException> repositoryResult1;
    private final CloseableIteration<? extends Statement, SailException> repositoryResult2;

    /**
     * create a combined CloseableIteration
     * @param repositoryResult1 the first iteration of element
     * @param repositoryResult2 the second iterator of element
     */
    public CombinedNativeStoreResult(CloseableIteration<? extends Statement, SailException> repositoryResult1,
                                     CloseableIteration<? extends Statement, SailException> repositoryResult2) {
        this.repositoryResult1 = repositoryResult1;
        this.repositoryResult2 = repositoryResult2;
    }

    @Override
    public boolean hasNext() {
        return this.repositoryResult1.hasNext() || repositoryResult2.hasNext();
    }

    @Override
    public Statement next() {
        if (repositoryResult1.hasNext()) {
            return this.repositoryResult1.next();
        }
        if (repositoryResult2.hasNext()) {
            return this.repositoryResult2.next();
        }
        return null;
    }

    @Override
    public void remove() throws SailException {
        if (repositoryResult1.hasNext()) {
            repositoryResult1.remove();
        } else if (repositoryResult2.hasNext()) {
            repositoryResult2.remove();
        }
    }

    @Override
    public void close() {
        this.repositoryResult1.close();
        this.repositoryResult2.close();
    }
}
