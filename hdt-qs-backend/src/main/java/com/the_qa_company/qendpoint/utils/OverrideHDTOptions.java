package com.the_qa_company.qendpoint.utils;

import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.options.HDTOptionsBase;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class OverrideHDTOptions implements HDTOptions {
	private final HDTOptions handle;
	private final Map<String, String> override = new HashMap<>();

	public OverrideHDTOptions(HDTOptions handle) {
		this.handle = Objects.requireNonNullElseGet(handle, HDTOptionsBase::new);
	}

	@Override
	public void clear() {
		handle.clear();
		override.clear();
	}

	@Override
	public String get(String s) {
		String v = override.get(s);
		return v == null ? handle.get(s) : v;
	}

	@Override
	public long getInt(String s) {
		String value = get(s);
		return value == null ? 0 : Integer.parseInt(value);
	}

	public void setOverride(String key, Object value) {
		if (value == null) {
			override.remove(key);
		} else {
			override.put(key, String.valueOf(value));
		}
	}

	@Override
	public void set(String s, String s1) {
		handle.set(s, s1);
	}

	@Override
	public void setInt(String s, long l) {
		handle.set(s, String.valueOf(l));
	}

	@Override
	public void setOptions(String s) {
		handle.setOptions(s);
	}
}
