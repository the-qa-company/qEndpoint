package org.rdfhdt.hdt.rdf4j.extensible;

import org.eclipse.rdf4j.IsolationLevels;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.ExceptionConvertingIteration;
import org.eclipse.rdf4j.common.iteration.LookAheadIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.extensiblestore.DataStructureInterface;
import org.eclipse.rdf4j.sail.extensiblestore.valuefactory.ExtensibleContextStatement;
import org.eclipse.rdf4j.sail.extensiblestore.valuefactory.ExtensibleStatement;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.rdfhdt.hdt.hdt.HDT;

import java.util.Collection;

public class MyDataStructure implements DataStructureInterface {

    NativeStore nativeStore;
    MyTripleSource tripleSource;
    MyDataStructure(NativeStore nativeStore, HDT hdt){
        this.nativeStore = nativeStore;
        this.tripleSource = new MyTripleSource(hdt,nativeStore);
    }
    public void addStatement(ExtensibleStatement extensibleStatement) {
        SailConnection connection = this.nativeStore.getConnection();

        connection.begin(IsolationLevels.NONE);
        connection.addStatement(extensibleStatement.getSubject(),
                extensibleStatement.getPredicate(),extensibleStatement.getObject());
        connection.commit();
    }
    public void removeStatement(ExtensibleStatement extensibleStatement) {

    }

    public void addStatement(Collection<ExtensibleStatement> statements) {

    }

    public void removeStatement(Collection<ExtensibleStatement> statements) {

    }
    @Override
    public CloseableIteration<? extends ExtensibleStatement, SailException> getStatements(Resource resource, IRI iri, Value value, boolean b, Resource... resources) {

        return new LookAheadIteration<ExtensibleStatement, SailException>() {
            CloseableIteration<? extends Statement, QueryEvaluationException> result =  tripleSource.getStatements(resource,iri,value,resources);
            @Override
            protected ExtensibleStatement getNextElement() throws SailException {
                ExtensibleStatement next = null;
                while (next == null && result.hasNext()){
                    Statement stm = result.next();
                    next = new ExtensibleContextStatement(stm.getSubject(),stm.getPredicate(),stm.getObject(),stm.getContext(),false);
                }
                return next;
            }
        };
    }

    public void flushForReading() {

    }

    public void init() {

    }

    public void clear(boolean inferred, Resource[] contexts) {

    }

    public void flushForCommit() {

    }

    public boolean removeStatementsByQuery(Resource subj, IRI pred, Value obj, boolean inferred, Resource[] contexts) {
        return false;
    }

    public long getEstimatedSize() {
        return 0;
    }
}
