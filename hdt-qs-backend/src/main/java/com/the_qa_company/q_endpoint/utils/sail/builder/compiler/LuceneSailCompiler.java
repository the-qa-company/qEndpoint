package com.the_qa_company.q_endpoint.utils.sail.builder.compiler;

import com.the_qa_company.q_endpoint.utils.sail.builder.SailCompiler;
import com.the_qa_company.q_endpoint.utils.sail.builder.SailCompilerSchema;
import com.the_qa_company.q_endpoint.utils.sail.helpers.LuceneSailBuilder;
import com.the_qa_company.q_endpoint.utils.sail.linked.LinkedSail;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.sail.NotifyingSail;
import org.eclipse.rdf4j.sail.evaluation.TupleFunctionEvaluationMode;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;
import org.eclipse.rdf4j.sail.lucene.LuceneSailSchema;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LuceneSailCompiler extends LinkedSailCompiler {
	private final Set<LuceneSail> sails = new HashSet<>();
	public LuceneSailCompiler() {
		super(SailCompilerSchema.LUCENE_TYPE);
	}

	public void reset() {
		sails.clear();
	}

	public Set<LuceneSail> getSails() {
		return sails;
	}

	@Override
	public LinkedSail<? extends NotifyingSail> compileWithParam(SailCompiler.SailCompilerReader reader, Resource rnode)
			throws SailCompiler.SailCompilerException {

		LuceneSailBuilder builder = new LuceneSailBuilder();

		reader.searchOneOpt(rnode, LuceneSailSchema.INDEXID)
				.map(SailCompiler::asIRI)
				.map(IRI::toString)
				.ifPresent(builder::withId);

		List<Value> languages = reader.search(rnode, SailCompilerSchema.LUCENE_TYPE_LANG);

		if (!languages.isEmpty()) {
			builder.withLanguageFiltering(languages.stream().map(SailCompiler::asLitString).toArray(String[]::new));
		}

		reader.searchOneOpt(rnode, SailCompilerSchema.LUCENE_TYPE_EVAL_MODE)
				.map(SailCompiler::asLitString)
				.map(TupleFunctionEvaluationMode::valueOf)
				.ifPresent(builder::withEvaluationMode);

		reader.searchOneOpt(rnode, SailCompilerSchema.DIR_LOCATION)
				.map(reader.getSailCompiler()::asDir)
				.ifPresent(builder::withDir);

		reader.searchOneOpt(rnode, SailCompilerSchema.LUCENE_TYPE_REINDEX_QUERY)
				.map(SailCompiler::asLitString)
				.ifPresent(builder::withReindexQuery);

		reader.search(rnode, SailCompilerSchema.LUCENE_TYPE_PARAM).stream()
				.map(SailCompiler::asResource)
				.forEach(rsnode -> {
					String key = SailCompiler.asLitString(reader.searchOne(rsnode, SailCompilerSchema.PARAM_KEY));
					String value = SailCompiler.asLitString(reader.searchOne(rsnode, SailCompilerSchema.PARAM_VALUE));
					builder.withParameter(key, value);
				});

		LinkedSail<LuceneSail> lucene = builder.buildLinked();
		sails.add(lucene.getSail());
		return lucene;
	}
}
