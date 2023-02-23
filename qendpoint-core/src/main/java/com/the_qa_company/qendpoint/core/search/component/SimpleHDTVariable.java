package com.the_qa_company.qendpoint.core.search.component;

import com.the_qa_company.qendpoint.core.hdt.HDT;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class SimpleHDTVariable implements HDTVariable {
	private static final AtomicLong ID_FETCHER = new AtomicLong();
	private final long id = ID_FETCHER.incrementAndGet();
	private final HDT hdt;
	private final String name;

	public SimpleHDTVariable(HDT hdt, String name) {
		this.hdt = Objects.requireNonNull(hdt, "hdt can't be null!");
		this.name = Objects.requireNonNull(name, "name can't be null");
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public HDTVariable copy() {
		return new SimpleHDTVariable(hdt, name);
	}

	public HDT getHdt() {
		return hdt;
	}

	@Override
	public String toString() {
		return stringValue();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof HDTVariable)) {
			return false;
		}
		HDTVariable v = (HDTVariable) obj;
		if (!Objects.equals(v.getName(), getName())) {
			return false;
		}
		return getName() != null || (obj instanceof SimpleHDTVariable && id == ((SimpleHDTVariable) obj).id);
	}

	@Override
	public int hashCode() {
		return Objects.hash(stringValue());
	}

	@Override
	public String stringValue() {
		return "?" + name;
	}
}
