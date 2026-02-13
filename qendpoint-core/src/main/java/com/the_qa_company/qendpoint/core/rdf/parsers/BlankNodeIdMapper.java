package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.string.ByteString;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Remaps blank node IDs by appending a unique suffix.
 */
public final class BlankNodeIdMapper {
	private final String uuid;
	private final AtomicLong counter = new AtomicLong();
	private final ConcurrentHashMap<ByteString, ByteString> mapping = new ConcurrentHashMap<>();

	private BlankNodeIdMapper(String uuid) {
		this.uuid = uuid;
	}

	public static BlankNodeIdMapper create() {
		return new BlankNodeIdMapper(UUID.randomUUID().toString());
	}

	public void remap(TripleString triple, boolean includeGraph) {
		triple.setSubject(remapValue(triple.getSubject()));
		triple.setObject(remapValue(triple.getObject()));
		if (includeGraph) {
			CharSequence graph = triple.getGraph();
			if (graph != null && graph.length() > 0) {
				triple.setGraph(remapValue(graph));
			}
		}
	}

	private CharSequence remapValue(CharSequence value) {
		if (!isBlankNode(value)) {
			return value;
		}
		ByteString key = ByteString.of(value);
		return mapping.computeIfAbsent(key, this::mint);
	}

	private ByteString mint(ByteString original) {
		long id = counter.incrementAndGet();
		String minted = original.toString() + "-" + uuid + "-" + id;
		return ByteString.copy(minted);
	}

	private static boolean isBlankNode(CharSequence value) {
		return value != null && value.length() > 1 && value.charAt(0) == '_' && value.charAt(1) == ':';
	}
}
