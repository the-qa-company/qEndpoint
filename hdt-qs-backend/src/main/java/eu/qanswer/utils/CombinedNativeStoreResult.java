package eu.qanswer.utils;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.sail.SailException;

public class CombinedNativeStoreResult implements CloseableIteration<Statement, SailException> {

    private CloseableIteration<? extends Statement, SailException> repositoryResult1;
    private CloseableIteration<? extends Statement, SailException> repositoryResult2;

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
        if (repositoryResult1.hasNext())
            return this.repositoryResult1.next();
        else if (repositoryResult2.hasNext())
            return this.repositoryResult2.next();
        return null;
    }

    @Override
    public void remove() throws SailException {

    }

    @Override
    public void close() {

    }
}
