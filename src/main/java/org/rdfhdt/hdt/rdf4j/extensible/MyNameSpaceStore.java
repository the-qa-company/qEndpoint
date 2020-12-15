package org.rdfhdt.hdt.rdf4j.extensible;

import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.sail.extensiblestore.NamespaceStoreInterface;

import java.util.Iterator;

public class MyNameSpaceStore implements NamespaceStoreInterface {
    public String getNamespace(String s) {
        return null;
    }

    public void setNamespace(String s, String s1) {

    }

    public void removeNamespace(String s) {

    }

    public void clear() {

    }

    public void init() {

    }

    public Iterator<SimpleNamespace> iterator() {
        return null;
    }
}
