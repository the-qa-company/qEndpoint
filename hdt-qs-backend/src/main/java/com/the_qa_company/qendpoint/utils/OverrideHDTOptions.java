package com.the_qa_company.qendpoint.utils;

import com.the_qa_company.qendpoint.core.options.HDTOptions;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class OverrideHDTOptions implements HDTOptions {
	private final HDTOptions handle;
	private final HDTOptions override = HDTOptions.of();

	public OverrideHDTOptions(HDTOptions handle) {
		this.handle = HDTOptions.ofNullable(handle);
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
			override.set(key, (String) null);
		} else {
			override.set(key, value);
		}
	}

	@Override
	public void set(String s, String s1) {
		handle.set(s, s1);
	}

	@Override
	public Set<?> getKeys() {
		Set<Object> keys = new HashSet<>();
		keys.addAll(handle.getKeys());
		keys.addAll(override.getKeys());
		return Collections.unmodifiableSet(keys);
	}
}
