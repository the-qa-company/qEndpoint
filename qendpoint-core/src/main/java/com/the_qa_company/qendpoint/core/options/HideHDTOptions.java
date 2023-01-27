package com.the_qa_company.qendpoint.core.options;

import java.util.*;
import java.util.function.Function;

/**
 * {@link HDTOptions} wrapper to redirect a key to another key
 *
 * @author Antoine Willerval
 */
public class HideHDTOptions implements HDTOptions {
	private final HDTOptions spec;
	private final Function<String, String> mapper;
	private final Map<String, String> customOptions = new HashMap<>();

	/**
	 * @param spec wrapped options
	 */
	public HideHDTOptions(HDTOptions spec) {
		this(spec, Function.identity());
	}

	/**
	 * @param spec   wrapped options
	 * @param mapper mapping function (key) {@literal ->} newKey?
	 */
	public HideHDTOptions(HDTOptions spec, Function<String, String> mapper) {
		this.spec = spec;
		this.mapper = mapper;
	}

	/**
	 * override a value from the wrapped, set to null to cancel the override
	 *
	 * @param key   key
	 * @param value value, null to disable the override
	 */
	public void overrideValue(String key, Object value) {
		if (value != null) {
			customOptions.put(key, String.valueOf(value));
		} else {
			customOptions.remove(key);
		}
	}

	@Override
	public Set<?> getKeys() {
		return spec.getKeys();
	}

	private String map(String key) {
		return Objects.requireNonNullElse(mapper.apply(key), "");
	}

	@Override
	public String get(String key) {
		String newKey = map(key);
		String overrideValue = customOptions.get(newKey);
		return overrideValue != null ? overrideValue : spec.get(newKey);
	}

	@Override
	public void set(String key, String value) {
		spec.set(map(key), value);
	}

	@Override
	public void clear() {
		spec.clear();
	}
}
