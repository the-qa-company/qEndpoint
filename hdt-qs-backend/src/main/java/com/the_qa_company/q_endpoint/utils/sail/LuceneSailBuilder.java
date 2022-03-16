package com.the_qa_company.q_endpoint.utils.sail;

import org.eclipse.rdf4j.sail.evaluation.TupleFunctionEvaluationMode;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;

public class LuceneSailBuilder {
	private LuceneSail sail;

	public LuceneSailBuilder() {
		// default config
		sail = new LuceneSail();
		sail.setParameter(LuceneSail.INDEX_CLASS_KEY, LuceneSail.DEFAULT_INDEX_CLASS);
	}

	public LuceneSailBuilder withParameter(String key, String value) {
		sail.setParameter(key, value);
		return this;
	}

	public LuceneSailBuilder withId(String id) {
		return withParameter(LuceneSail.INDEX_ID, id);
	}

	public LuceneSailBuilder withLanguageFiltering(String... languages) {
		return withParameter(LuceneSail.INDEXEDLANG, String.join(" ", languages));
	}

	public LuceneSailBuilder withDir(String dir) {
		return withParameter(LuceneSail.LUCENE_DIR_KEY, dir);
	}

	public LuceneSailBuilder withEvaluationMode(TupleFunctionEvaluationMode mode) {
		sail.setEvaluationMode(mode);
		return this;
	}

	public LuceneSail build() {
		if (sail == null) {
			throw new IllegalArgumentException("build() method was already called before!");
		}
		LuceneSail tmp = sail;
		sail = null;
		return tmp;
	}
}
