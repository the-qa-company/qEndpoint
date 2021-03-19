package org.rdfhdt.hdt.rdf4j.misc;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryPreparer;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.parser.QueryParser;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.base.AbstractRepository;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.rdf4j.HybridTripleSource;

import java.io.File;
import java.io.IOException;

public class HDTRepository extends AbstractRepository {
  private File file;
  private HDT hdt;
  private boolean quadMode;
  private QueryParser queryParser = new SPARQLParser();
  private HybridTripleSource tripleSource;
  private QueryPreparer queryPreparer;

  public HDTRepository(File file) {
    this.file = file;
  }

  public HDTRepository(HDT hdt) {
    this.hdt = hdt;
  }

  protected void initializeInternal() throws RepositoryException {
    try {
      if (file != null) {
        HDTSpecification spec = new HDTSpecification();
        //spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj");

        hdt = HDTManager.mapIndexedHDT(file.getAbsolutePath(),spec);
      }
      // TODO: adapt this for tests....
      //tripleSource = new HDTTripleSource(hdt);
      queryPreparer = new HDTQueryPreparer(tripleSource);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  protected void shutDownInternal() throws RepositoryException {
    try {
      hdt.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void setDataDir(File file) {
    this.file = file;
  }

  public File getDataDir() {
    return file;
  }

  public boolean isWritable() throws RepositoryException {
    return false;
  }

  public RepositoryConnection getConnection() throws RepositoryException {
    return new HDTRepositoryConnection(this);
  }

  public ValueFactory getValueFactory() {
    return SimpleValueFactory.getInstance();
  }

  QueryPreparer getQueryPreparer() {
    return queryPreparer;
  }

  QueryParser getQueryParser() {
    return queryParser;
  }

  TripleSource getTripleSource() {
    return tripleSource;
  }
}
