package com.the_qa_company.q_endpoint.utils.sail.helpers;

import com.the_qa_company.q_endpoint.utils.sail.linked.LinkedSail;
import com.the_qa_company.q_endpoint.utils.sail.linked.SimpleLinkedSail;
import org.eclipse.rdf4j.sail.evaluation.TupleFunctionEvaluationMode;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;

/**
 * helper class to create a {@link org.eclipse.rdf4j.sail.lucene.LuceneSail} linked or not
 * @author Antoine Willerval
 */
public class LuceneSailBuilder {
	private LuceneSail sail;

	public LuceneSailBuilder() {
		// default config
		sail = new LuceneSail();
		sail.setParameter(LuceneSail.INDEX_CLASS_KEY, LuceneSail.DEFAULT_INDEX_CLASS);
	}

	/**
	 * add a particular parameter
	 * @param key parameter key
	 * @param value parameter value
	 * @return this
	 */
	public LuceneSailBuilder withParameter(String key, String value) {
		sail.setParameter(key, value);
		return this;
	}

	/**
	 * set the id of the sail
	 * @param id the id of the store
	 * @return this
	 */
	public LuceneSailBuilder withId(String id) {
		return withParameter(LuceneSail.INDEX_ID, id);
	}

	/**
	 * set the language(s) of the sail
	 * @param languages the language(s) of the store
	 * @return this
	 */
	public LuceneSailBuilder withLanguageFiltering(String... languages) {
		return withParameter(LuceneSail.INDEXEDLANG, String.join(" ", languages));
	}

	/**
	 * set the directory of the sail
	 * @param dir the directory
	 * @return this
	 */
	public LuceneSailBuilder withDir(String dir) {
		return withParameter(LuceneSail.LUCENE_DIR_KEY, dir);
	}

	/**
	 * set the evaluation mode of the sail
	 * @param mode the mode
	 * @return this
	 */
	public LuceneSailBuilder withEvaluationMode(TupleFunctionEvaluationMode mode) {
		sail.setEvaluationMode(mode);
		return this;
	}

	/**
	 * set the query reindexing query
	 * @param query the query
	 * @return this
	 */
	public LuceneSailBuilder withReindexQuery(String query) {
		sail.setReindexQuery(query);
		return this;
	}

	/**
	 * build the sail, can't be called twice
	 * @return the sail
	 */
	public LuceneSail build() {
		if (sail == null) {
			throw new IllegalArgumentException("build() method was already called before!");
		}
		LuceneSail tmp = sail;
		sail = null;
		return tmp;
	}

	/**
	 * build the sail as a {@link com.the_qa_company.q_endpoint.utils.sail.linked.LinkedSail}, can't be called twice.
	 * @return the sail
	 */
	public LinkedSail<LuceneSail> buildLinked() {
		LuceneSail sail = build();
		return new SimpleLinkedSail<>(sail, sail::setBaseSail);
	}
}
