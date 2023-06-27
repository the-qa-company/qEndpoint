package com.the_qa_company.qendpoint.core.enums;

public enum RDFNodeType {
	IRI, BLANK_NODE, LITERAL;

	public static RDFNodeType typeof(CharSequence node) {
		if (node == null || node.isEmpty()) {
			return null;
		}

		if (node.charAt(0) == '"') {
			return RDFNodeType.LITERAL;
		}
		if (node.length() >= 2 && node.charAt(0) == '_' && node.charAt(1) == ':') {
			return RDFNodeType.BLANK_NODE;
		}
		return RDFNodeType.IRI;
	}
}
