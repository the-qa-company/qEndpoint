package com.the_qa_company.qendpoint.utils.sail.builder.compiler;

import com.the_qa_company.qendpoint.utils.sail.FilteringSail;
import com.the_qa_company.qendpoint.utils.sail.builder.SailCompiler;
import com.the_qa_company.qendpoint.utils.sail.builder.SailCompilerSchema;
import com.the_qa_company.qendpoint.utils.sail.filter.LanguageSailFilter;
import com.the_qa_company.qendpoint.utils.sail.filter.LuceneMatchExprSailFilter;
import com.the_qa_company.qendpoint.utils.sail.filter.PredicateSailFilter;
import com.the_qa_company.qendpoint.utils.sail.filter.SailFilter;
import com.the_qa_company.qendpoint.utils.sail.filter.TypeSailFilter;
import com.the_qa_company.qendpoint.utils.sail.linked.LinkedSail;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.sail.NotifyingSail;
import org.eclipse.rdf4j.sail.SailConnection;

import java.util.List;
import java.util.function.BiFunction;

/**
 * a linked sail compiler to create filtering sail
 * @author Antoine Willerval
 */
public class FilterLinkedSailCompiler extends LinkedSailCompiler {
	public FilterLinkedSailCompiler() {
		super(SailCompilerSchema.FILTER_TYPE);
	}

	@Override
	public LinkedSail<? extends NotifyingSail> compileWithParam(SailCompiler.SailCompilerReader reader, Resource rnode)
			throws SailCompiler.SailCompilerException {
		return new FilteringSail(
				reader.compileNode(reader.searchOne(rnode, SailCompilerSchema.PARAM_LINK)),
				compileFilter(reader, SailCompiler.asResource(reader.searchOne(rnode, SailCompilerSchema.PARAM_FILTER)))
		);
	}

	/**
	 * add to a filter builder function AND and OR filter
	 *
	 * @param reader   the reader
	 * @param rnode    the filter node
	 * @param function the filter builder
	 * @return the filter build with and/or method
	 * @throws SailCompiler.SailCompilerException compiler error
	 */
	private BiFunction<FilteringSail, SailConnection, SailFilter> combineCompileFilter(SailCompiler.SailCompilerReader reader, Resource rnode,
																	  BiFunction<FilteringSail, SailConnection, SailFilter> function)
			throws SailCompiler.SailCompilerException {
		List<Value> predicatesAnd = reader.search(rnode, SailCompilerSchema.PARAM_FILTER_AND);
		List<Value> predicatesOr = reader.search(rnode, SailCompilerSchema.PARAM_FILTER_OR);

		for (Value snode : predicatesAnd) {
			Resource rsnode = SailCompiler.asResource(snode);
			BiFunction<FilteringSail, SailConnection, SailFilter> f1 = function;
			BiFunction<FilteringSail, SailConnection, SailFilter> f2 = compileFilter(reader, rsnode);
			function = (sail, connection) -> f1.apply(sail, connection).and(f2.apply(sail, connection));
		}

		for (Value snode : predicatesOr) {
			Resource rsnode = SailCompiler.asResource(snode);
			BiFunction<FilteringSail, SailConnection, SailFilter> f1 = function;
			BiFunction<FilteringSail, SailConnection, SailFilter> f2 = compileFilter(reader, rsnode);
			function = (sail, connection) -> f1.apply(sail, connection).or(f2.apply(sail, connection));
		}

		return function;
	}

	/**
	 * compile a filter from a reader
	 *
	 * @param reader the reader
	 * @param rnode  the filter node
	 * @return filter builder function
	 * @throws SailCompiler.SailCompilerException compiler error
	 */
	private BiFunction<FilteringSail, SailConnection, SailFilter> compileFilter(SailCompiler.SailCompilerReader reader, Resource rnode)
			throws SailCompiler.SailCompilerException {
		IRI type = SailCompiler.asIRI(reader.searchOne(rnode, SailCompilerSchema.TYPE));
		BiFunction<FilteringSail, SailConnection, SailFilter> function;

		if (type.equals(SailCompilerSchema.PARAM_FILTER_TYPE_PREDICATE)) {
			IRI predicate = SailCompiler.asIRI(reader.searchOne(rnode, SailCompilerSchema.PARAM_FILTER_TYPE_TYPE_PREDICATE));
			PredicateSailFilter filter = new PredicateSailFilter(predicate);
			function = (sail, connection) -> filter;
		} else if (type.equals(SailCompilerSchema.PARAM_FILTER_TYPE_LANGUAGE)) {
			String lang = reader.getSailCompiler().asLitString(reader.searchOne(rnode, SailCompilerSchema.PARAM_FILTER_TYPE_LANGUAGE_LANG));
			boolean acceptNoLanguageLiterals = reader.searchOneOpt(rnode, SailCompilerSchema.PARAM_FILTER_TYPE_LANGUAGE_NO_LANG_LIT).isPresent();
			LanguageSailFilter filter = new LanguageSailFilter(lang, acceptNoLanguageLiterals);
			function = (sail, connection) -> filter;
		} else if (type.equals(SailCompilerSchema.PARAM_FILTER_TYPE_TYPE)) {
			IRI predicate = SailCompiler.asIRI(reader.searchOne(rnode, SailCompilerSchema.PARAM_FILTER_TYPE_TYPE_PREDICATE));
			Value object = reader.searchOne(rnode, SailCompilerSchema.PARAM_FILTER_TYPE_TYPE_OBJECT);
			function = (sail, connection) -> new TypeSailFilter(sail, predicate, object);
		} else if (type.equals(SailCompilerSchema.PARAM_FILTER_TYPE_LUCENE_EXP)) {
			LuceneMatchExprSailFilter filter = new LuceneMatchExprSailFilter();
			function = (sail, connection) -> filter;
		} else {
			throw new SailCompiler.SailCompilerException("Can't find the filter type " + type);
		}

		return combineCompileFilter(reader, rnode, function);
	}
}
