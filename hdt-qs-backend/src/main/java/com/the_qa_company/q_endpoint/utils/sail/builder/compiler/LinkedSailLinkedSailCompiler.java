package com.the_qa_company.q_endpoint.utils.sail.builder.compiler;

import com.the_qa_company.q_endpoint.utils.sail.builder.SailCompiler;
import com.the_qa_company.q_endpoint.utils.sail.builder.SailCompilerSchema;
import com.the_qa_company.q_endpoint.utils.sail.linked.LinkedSail;
import com.the_qa_company.q_endpoint.utils.sail.linked.SimpleLinkedSail;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.sail.NotifyingSail;

import java.util.List;

public class LinkedSailLinkedSailCompiler extends LinkedSailCompiler {
	public LinkedSailLinkedSailCompiler() {
		super(SailCompilerSchema.LINKED_SAIL_TYPE);
	}

	@Override
	public LinkedSail<? extends NotifyingSail> compileWithParam(SailCompiler.SailCompilerReader reader, Resource rnode)
			throws SailCompiler.SailCompilerException {
		List<Value> nodes = reader.search(rnode, SailCompilerSchema.NODE);
		return SimpleLinkedSail.linkSails(nodes.stream().map(reader::compileNode));
	}
}
