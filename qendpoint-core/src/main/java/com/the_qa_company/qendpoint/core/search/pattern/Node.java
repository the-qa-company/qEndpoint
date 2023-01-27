package com.the_qa_company.qendpoint.core.search.pattern;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Node<T> {
	private final T value;
	private final Map<String, Set<Node<T>>> links = new HashMap<>();

	public Node(T value) {
		this.value = value;
	}

	public void addLink(String variable, Node<T> node) {
		links.computeIfAbsent(variable, key -> new HashSet<>()).add(node);
	}

	public Map<String, Set<Node<T>>> getLinks() {
		return links;
	}

	public T getValue() {
		return value;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		Node<?> node = (Node<?>) o;
		return Objects.equals(value, node.value) && Objects.equals(links, node.links);
	}

	@Override
	public int hashCode() {
		return Objects.hash(value, links);
	}
}
