package com.the_qa_company.qendpoint.core.storage;

import com.the_qa_company.qendpoint.core.dictionary.TempDictionary;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.hdt.TempHDT;
import com.the_qa_company.qendpoint.core.hdt.impl.ModeOfLoading;
import com.the_qa_company.qendpoint.core.hdt.impl.TempHDTImpl;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.storage.iterator.QueryCloseableIterator;
import com.the_qa_company.qendpoint.core.triples.TempTriples;
import com.the_qa_company.qendpoint.core.triples.TripleString;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * one-pass memory importer
 *
 * @author Antoine Willerval
 */
public class QEPImporter {
	private final QEPCore core;
	private TempHDT tempHDT;
	private final Set<TripleString> deleteData = new HashSet<>();
	private long rdfSize;
	private final Object lock = new Object() {};

	QEPImporter(QEPCore core) {
		this.core = core;
	}

	/**
	 * start a transaction of no one is active
	 */
	public void startTransaction() {
		synchronized (lock) {
			if (!isTransactionActive()) {
				begin();
			}
		}
	}

	/**
	 * end a transaction if one is active
	 *
	 * @param commit commit the transaction, otherwise it will rollback it
	 */
	public void endTransaction(boolean commit) {
		synchronized (lock) {
			if (isTransactionActive()) {
				if (commit) {
					commit();
				} else {
					rollback();
				}
			}
		}
	}

	/**
	 * @return if a transaction is active
	 */
	public boolean isTransactionActive() {
		synchronized (lock) {
			return tempHDT != null;
		}
	}

	/**
	 * begin a new transaction
	 *
	 * @throws QEPCoreException if a transaction is already active
	 */
	public void begin() throws QEPCoreException {
		synchronized (lock) {
			if (isTransactionActive()) {
				throw new QEPCoreException("The importer is already started! You must commit or rollback the result!");
			}

			TempHDT modHDT = new TempHDTImpl(core.getOptions(), "http://the-qa-company.com/qendpoint/qepds/",
					ModeOfLoading.ONE_PASS);
			modHDT.getDictionary().startProcessing();
			tempHDT = modHDT;
			rdfSize = 0L;
			deleteData.clear();
		}
	}

	/**
	 * commit a transaction
	 *
	 * @throws QEPCoreException if no transaction is active
	 */
	public void commit() throws QEPCoreException {
		synchronized (lock) {
			if (!isTransactionActive()) {
				throw new QEPCoreException("The importer isn't started! Please use begin to start a transaction.");
			}

			// clear duplicated
			if (rdfSize > 0) { // we add at least 4 bytes/triple
				tempHDT.getDictionary().endProcessing();

				// Reorganize both the dictionary and the triples
				tempHDT.reorganizeDictionary(ProgressListener.ignore());
				tempHDT.reorganizeTriples(ProgressListener.ignore());

				tempHDT.getHeader().insert("_:statistics", HDTVocabulary.ORIGINAL_SIZE, rdfSize);

				try {
					try {
						core.insertTriples(tempHDT, true);
					} catch (IOException e) {
						throw new QEPCoreException(e);
					}
				} catch (Throwable t) {
					try {
						tempHDT.close();
					} catch (Exception e) {
						t.addSuppressed(e);
					} catch (Throwable t2) {
						t2.addSuppressed(t);
						throw t2;
					}
					throw t;
				}
				try {
					tempHDT.close();
				} catch (IOException e) {
					throw new QEPCoreException(e);
				}
				// remove the triples from the core, it should be done after the
				// add to avoid collisions
				QEPCoreContext ctx = core.createSearchContext();
				for (TripleString deleteDatum : deleteData) {
					core.removeTriple(ctx, deleteDatum);
				}
			}

			tempHDT = null;
			deleteData.clear();
		}
	}

	/**
	 * insert a triple
	 *
	 * @param triple triple
	 * @throws QEPCoreException if no transaction is active
	 */
	public void insertTriple(TripleString triple) throws QEPCoreException {
		synchronized (lock) {
			if (!isTransactionActive()) {
				throw new QEPCoreException("The importer isn't started! Please use begin to start a transaction.");
			}

			triple = triple.tripleToByteStringCast();

			// check if it is already in the store

			// check it wasn't removed
			deleteData.remove(triple);

			TempTriples triples = tempHDT.getTriples();
			TempDictionary dict = tempHDT.getDictionary();

			if (core.containsAny(triple)) {
				return;
			}

			triples.insert(dict.insert(triple.getSubject(), TripleComponentRole.SUBJECT),
					dict.insert(triple.getPredicate(), TripleComponentRole.PREDICATE),
					dict.insert(triple.getObject(), TripleComponentRole.OBJECT));
			rdfSize += triple.getSubject().length() + triple.getPredicate().length() + triple.getObject().length() + 4; // Spaces
		}
	}

	public void deleteTriple(TripleString ts) throws QEPCoreException {
		synchronized (lock) {
			if (!isTransactionActive()) {
				throw new QEPCoreException("The importer isn't started! Please use begin to start a transaction.");
			}

			try (QueryCloseableIterator search = core.search(ts)) {
				if (ts.isStatic()) {
					if (search.hasNext()) {
						deleteData.add(ts.tripleToByteString());
					}
				} else {
					while (search.hasNext()) {
						deleteData.add(search.next().tripleString().tripleToByteString());
					}
				}
			}
		}
	}

	/**
	 * rollback a transaction
	 *
	 * @throws QEPCoreException if no transaction is active
	 */
	public void rollback() throws QEPCoreException {
		synchronized (lock) {
			if (!isTransactionActive()) {
				throw new QEPCoreException("The importer isn't started! Please use begin to start a transaction.");
			}
			deleteData.clear();

			try {
				tempHDT.close();
			} catch (IOException e) {
				throw new QEPCoreException(e);
			}
			tempHDT = null;
			rdfSize = 0;
		}
	}
}
