package com.the_qa_company.qendpoint.utils.sail.builder.compiler;

import com.the_qa_company.qendpoint.utils.sail.MultiTypeFilteringSail;
import com.the_qa_company.qendpoint.utils.sail.builder.SailCompiler;
import com.the_qa_company.qendpoint.utils.sail.builder.SailCompilerSchema;
import com.the_qa_company.qendpoint.utils.sail.linked.LinkedSail;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.sail.NotifyingSail;

import java.util.stream.Collectors;

/**
 * a linked sail compiler to create a multi-filter sail
 * @author Antoine Willerval
 */
public class MultiFilterLinkedSailCompiler extends LinkedSailCompiler {
	public MultiFilterLinkedSailCompiler() {
		super(SailCompilerSchema.MULTI_FILTER_TYPE);
	}

	@Override
	public LinkedSail<? extends NotifyingSail> compileWithParam(SailCompiler.SailCompilerReader reader, Resource rnode)
			throws SailCompiler.SailCompilerException {

		return new MultiTypeFilteringSail(
				SailCompiler.asIRI(reader.searchOne(rnode, SailCompilerSchema.PARAM_FILTER_TYPE_TYPE_PREDICATE)),
				reader.search(rnode, SailCompilerSchema.NODE).stream()
						.map(rsnode -> typedSailOf(
								reader.compileNode(
										reader.searchOne(SailCompiler.asResource(rsnode), SailCompilerSchema.NODE)
								),
								SailCompiler.asIRI(
										reader.searchOne(
												SailCompiler.asResource(rsnode),
												SailCompilerSchema.PARAM_FILTER_TYPE_TYPE_OBJECT
										)
								)
						)).collect(Collectors.toList())

		);
	}

	public MultiTypeFilteringSail.TypedSail typedSailOf(LinkedSail<? extends NotifyingSail> sail, IRI type) {
		return new MultiTypeFilteringSail.TypedSail(sail, type);
	}
}
