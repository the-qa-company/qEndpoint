package com.the_qa_company.qendpoint.utils.sail.helpers;

import com.the_qa_company.qendpoint.compiler.SailCompilerSchema;
import com.the_qa_company.qendpoint.utils.sail.linked.LinkedSail;
import com.the_qa_company.qendpoint.utils.sail.linked.SimpleLinkedSail;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.sail.evaluation.TupleFunctionEvaluationMode;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * helper class to create a {@link org.eclipse.rdf4j.sail.lucene.LuceneSail} linked or not
 * @author Antoine Willerval
 */
public class LuceneSailBuilder {
	private final Map<String, String> params = new HashMap<>();
	private TupleFunctionEvaluationMode eval;
	private String reindexQuery;

	/**
	 * create a builder with default config
	 */
	public LuceneSailBuilder() {
		// default config
		withParameter(LuceneSail.INDEX_CLASS_KEY, LuceneSail.DEFAULT_INDEX_CLASS);
	}

	/**
	 * add a particular parameter
	 * @param key parameter key
	 * @param value parameter value
	 * @return this
	 */
	public LuceneSailBuilder withParameter(String key, String value) {
		params.put(key, value);
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
		eval = mode;
		return this;
	}

	/**
	 * set the query reindexing query
	 * @param query the query
	 * @return this
	 */
	public LuceneSailBuilder withReindexQuery(String query) {
		reindexQuery = query;
		return this;
	}

	/**
	 * build the sail, can't be called twice
	 * @return the sail
	 */
	public LuceneSail build() {
		LuceneSail sail = new LuceneSail();
		params.forEach(sail::setParameter);
		if (eval != null) {
			sail.setEvaluationMode(eval);
		}
		if (reindexQuery != null) {
			sail.setReindexQuery(reindexQuery);
		}
		return sail;
	}

	/**
	 * build this lucene sail to rdf for compiling with the {@link com.the_qa_company.qendpoint.compiler.SailCompiler}
	 * @param node node describing this sail
	 * @return iterator of statements
	 */
	public Iterator<Statement> buildToRDF(Resource node) {
		ValueFactory vf = SimpleValueFactory.getInstance();
		List<Statement> list = new ArrayList<>();
		// mdlc:type mdlc:luceneNode
		list.add(vf.createStatement(node, SailCompilerSchema.TYPE, SailCompilerSchema.LUCENE_TYPE));
		params.forEach((key, value) -> {
			// mdlc:luceneParam [...]
			BNode param = vf.createBNode();
			list.add(vf.createStatement(node, SailCompilerSchema.LUCENE_TYPE_PARAM, param));
			list.add(vf.createStatement(param, SailCompilerSchema.PARAM_KEY, vf.createLiteral(key)));
			list.add(vf.createStatement(param, SailCompilerSchema.PARAM_VALUE, vf.createLiteral(key)));
		});
		// mdlc:luceneEvalMode "MODE"
		if (eval != null) {
			list.add(vf.createStatement(node, SailCompilerSchema.LUCENE_TYPE_EVAL_MODE, vf.createLiteral(eval.name())));
		}
		// mdlc:luceneReindexQuery "SELECT ..."
		if (reindexQuery != null) {
			list.add(vf.createStatement(node, SailCompilerSchema.LUCENE_TYPE_REINDEX_QUERY, vf.createLiteral(reindexQuery)));
		}
		return list.iterator();
	}

	/**
	 * build the sail as a {@link com.the_qa_company.qendpoint.utils.sail.linked.LinkedSail}, can't be called twice.
	 * @return the sail
	 */
	public LinkedSail<LuceneSail> buildLinked() {
		LuceneSail sail = build();
		return new SimpleLinkedSail<>(sail, sail::setBaseSail);
	}
}
