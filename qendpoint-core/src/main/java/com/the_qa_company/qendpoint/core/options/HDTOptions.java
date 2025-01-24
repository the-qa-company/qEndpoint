/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/iface/org/rdfhdt/hdt/
 * options/HDTOptions.java $ Revision: $Rev: 191 $ Last modified: $Date:
 * 2013-03-03 11:41:43 +0000 (dom, 03 mar 2013) $ Last modified by: $Author:
 * mario.arias $ This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License,
 * or (at your option) any later version. This library is distributed in the
 * hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * library; if not, write to the Free Software Foundation, Inc., 51 Franklin St,
 * Fifth Floor, Boston, MA 02110-1301 USA Contacting the authors: Mario Arias:
 * mario.arias@deri.org Javier D. Fernandez: jfergar@infor.uva.es Miguel A.
 * Martinez-Prieto: migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.options;

import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.rdf.RDFFluxStop;
import com.the_qa_company.qendpoint.core.util.Profiler;
import com.the_qa_company.qendpoint.core.util.UnicodeEscape;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Options storage, see {@link HDTOptionsKeys} for more information.
 *
 * @author mario.arias
 */
public interface HDTOptions {
	/**
	 * empty option, can't be used to set values
	 */
	HDTOptions EMPTY = new HDTOptions() {
		@Override
		public void clear() {
			// already empty
		}

		@Override
		public String get(String key) {
			// no value for key
			return null;
		}

		@Override
		public void set(String key, String value) {
			throw new NotImplementedException("set");
		}

		@Override
		public Set<?> getKeys() {
			return Collections.emptySet();
		}
	};

	/**
	 * @return get empty unmodifiable options
	 */
	static HDTOptions empty() {
		return EMPTY;
	}

	/**
	 * create modifiable options from a clone of another option
	 *
	 * @param other other option to clone
	 * @return options
	 */
	static HDTOptions of(HDTOptions other) {
		HDTOptions opt = of();
		opt.setOptions(other);
		return opt;
	}

	/**
	 * create modifiable options
	 *
	 * @return options
	 */
	static HDTOptions of() {
		Map<String, String> map = new TreeMap<>();
		return new HDTOptions() {
			@Override
			public void clear() {
				map.clear();
			}

			@Override
			public String get(String key) {
				return map.get(key);
			}

			@Override
			public void set(String key, String value) {
				map.put(key, value);
			}

			@Override
			public Set<String> getKeys() {
				return Collections.unmodifiableSet(map.keySet());
			}
		};
	}

	/**
	 * create modifiable options starting from the copy of the data map
	 *
	 * @param data data map
	 * @return options
	 */
	static HDTOptions of(Map<?, ?> data) {
		Objects.requireNonNull(data, "data map can't be null!");
		HDTOptions opt = of();
		opt.setOptions(data);
		return opt;
	}

	/**
	 * create modifiable options starting from initial config, each param should
	 * be in the format (key, value)*
	 *
	 * @param data data map
	 * @return options
	 * @throws IllegalArgumentException if the number of param isn't even
	 */
	static HDTOptions of(Object... data) {
		Objects.requireNonNull(data, "data can't be null!");
		HDTOptions opt = of();
		opt.setOptions(data);
		return opt;
	}

	/**
	 * create modifiable options with a string config
	 *
	 * @param cfg config
	 * @return options
	 */
	static HDTOptions of(String cfg) {
		Objects.requireNonNull(cfg, "cfg can't be null!");
		HDTOptions opt = of();
		opt.setOptions(cfg);
		return opt;
	}

	/**
	 * get options or {@link #EMPTY}
	 *
	 * @param options options
	 * @return options or {@link #EMPTY}, this result has no guaranty or
	 *         mutability
	 */
	static HDTOptions ofNullable(HDTOptions options) {
		return Objects.requireNonNullElse(options, EMPTY);
	}

	/**
	 * create modifiable options from a file configuration
	 *
	 * @param filename file containing the options, see {@link #load(Path)}
	 * @return options
	 */
	static HDTOptions readFromFile(Path filename) throws IOException {
		return HDTManager.readOptions(Objects.requireNonNull(filename, "filename can't be null!"));
	}

	/**
	 * create modifiable options from a file configuration
	 *
	 * @param filename file containing the options, see {@link #load(String)}
	 * @return options
	 */
	static HDTOptions readFromFile(String filename) throws IOException {
		// use readOptions to have access to HTTP(s) files
		return HDTManager.readOptions(Objects.requireNonNull(filename, "filename can't be null!"));
	}

	/**
	 * clear all the options
	 */
	void clear();

	/**
	 * get an option value
	 *
	 * @param key key
	 * @return value or null if not defined
	 */
	String get(String key);

	/**
	 * check if an option is valid
	 *
	 * @param key key
	 * @return true if null or empty, false otherwise
	 */
	default boolean contains(String key) {
		String s = get(key);
		return s != null && !s.isEmpty();
	}

	/**
	 * Get a path value
	 *
	 * @param key key
	 * @return path or null if not defined
	 */
	default Path getPath(String key) {
		return getPath(key, (Path) null);
	}

	/**
	 * Get a path value
	 *
	 * @param key          key
	 * @param defaultValue default value
	 * @return path or defaultValue if not defined
	 */
	default Path getPath(String key, Path defaultValue) {
		return getPath(key, () -> defaultValue);
	}

	/**
	 * Get a path value
	 *
	 * @param key          key
	 * @param defaultValue default value
	 * @return path or defaultValue if not defined
	 */
	default Path getPath(String key, Supplier<Path> defaultValue) {
		String val = get(key);
		if (val == null) {
			return defaultValue.get();
		}
		return Path.of(val);
	}

	/**
	 * @return the keys of the options
	 * @throws NotImplementedException if the implementation does not implement
	 *                                 this method (backward compatibility)
	 */
	default Set<?> getKeys() {
		throw new NotImplementedException("getKeys");
	}

	/**
	 * @return the options to be used with {@link #setOptions(String)}
	 */
	default String getOptions() {
		StringBuilder bld = new StringBuilder();

		for (Object k : getKeys()) {
			String keyStr = String.valueOf(k);
			bld.append(keyStr).append("=").append(get(keyStr)).append(";");
		}

		return bld.toString();
	}

	/**
	 * get a value
	 *
	 * @param key          key
	 * @param defaultValue default value
	 * @return value or defaultValue if the value isn't defined
	 */
	default String get(String key, String defaultValue) {
		return Objects.requireNonNullElse(get(key), defaultValue);
	}

	/**
	 * get a value
	 *
	 * @param key          key
	 * @param defaultValue default value
	 * @return value or defaultValue if the value isn't defined
	 */
	default String get(String key, Supplier<String> defaultValue) {
		return Objects.requireNonNullElseGet(get(key), defaultValue);
	}

	/**
	 * get a boolean
	 *
	 * @param key key
	 * @return boolean or false if the value isn't defined
	 */
	default boolean getBoolean(String key) {
		return "true".equalsIgnoreCase(get(key));
	}

	/**
	 * get a boolean
	 *
	 * @param key          key
	 * @param defaultValue default value
	 * @return boolean or false if the value isn't defined
	 */
	default boolean getBoolean(String key, boolean defaultValue) {
		String v = get(key);
		if (v == null) {
			return defaultValue;
		}
		return "true".equalsIgnoreCase(v);
	}

	/**
	 * get a double
	 *
	 * @param key key
	 * @return double or 0 if the value isn't defined
	 */
	default double getDouble(String key) {
		return getDouble(key, 0);
	}

	/**
	 * get a double
	 *
	 * @param key          key
	 * @param defaultValue default value
	 * @return double or defaultValue if the value isn't defined
	 */
	default double getDouble(String key, DoubleSupplier defaultValue) {
		String l = get(key);
		if (l == null) {
			return defaultValue.getAsDouble();
		}
		return Double.parseDouble(l);
	}

	/**
	 * get a double
	 *
	 * @param key          key
	 * @param defaultValue default value
	 * @return double or defaultValue if the value isn't defined
	 */
	default double getDouble(String key, double defaultValue) {
		return getDouble(key, () -> defaultValue);
	}

	/**
	 * get an {@link RDFFluxStop}
	 *
	 * @param key key
	 * @return RDFFluxStop or false if the value isn't defined
	 */
	default RDFFluxStop getFluxStop(String key) {
		return RDFFluxStop.readConfig(get(key));
	}

	/**
	 * get an {@link RDFFluxStop}
	 *
	 * @param key          key
	 * @param defaultValue default value
	 * @return RDFFluxStop or defaultValue if the value isn't defined
	 */
	default RDFFluxStop getFluxStop(String key, Supplier<RDFFluxStop> defaultValue) {
		return Objects.requireNonNullElseGet(getFluxStop(key), defaultValue);
	}

	/**
	 * get an {@link RDFFluxStop}
	 *
	 * @param key          key
	 * @param defaultValue default value
	 * @return RDFFluxStop or defaultValue if the value isn't defined
	 */
	default RDFFluxStop getFluxStop(String key, RDFFluxStop defaultValue) {
		return getFluxStop(key, () -> defaultValue);
	}

	/**
	 * get a long value
	 *
	 * @param key key
	 * @return value or 0 if not defined
	 */
	default long getInt(String key) {
		return getInt(key, 0);
	}

	/**
	 * get a long
	 *
	 * @param key          key
	 * @param defaultValue default value
	 * @return long or defaultValue if the value isn't defined
	 */
	default long getInt(String key, LongSupplier defaultValue) {
		String l = get(key);
		if (l == null) {
			return defaultValue.getAsLong();
		}
		return Long.parseLong(l);
	}

	/**
	 * get a long
	 *
	 * @param key          key
	 * @param defaultValue default value
	 * @return long or defaultValue if the value isn't defined
	 */
	default long getInt(String key, long defaultValue) {
		return getInt(key, () -> defaultValue);
	}

	/**
	 * get a long value
	 *
	 * @param key key
	 * @return value or 0 if not defined
	 */
	default int getInt32(String key) {
		return getInt32(key, 0);
	}

	/**
	 * get a long
	 *
	 * @param key          key
	 * @param defaultValue default value
	 * @return long or defaultValue if the value isn't defined
	 */
	default int getInt32(String key, IntSupplier defaultValue) {
		String l = get(key);
		if (l == null) {
			return defaultValue.getAsInt();
		}
		return Integer.parseInt(l);
	}

	/**
	 * get a long
	 *
	 * @param key          key
	 * @param defaultValue default value
	 * @return long or defaultValue if the value isn't defined
	 */
	default int getInt32(String key, int defaultValue) {
		return getInt32(key, () -> defaultValue);
	}

	/**
	 * load properties from a path, see {@link Properties#load(InputStream)} for
	 * the format
	 *
	 * @param filename file
	 * @throws IOException load io exception
	 */
	default void load(Path filename) throws IOException {
		Objects.requireNonNull(filename, "filename can't be null");
		Properties properties = new Properties();

		try (InputStream is = Files.newInputStream(filename)) {
			properties.load(is);
		}

		properties.forEach((k, v) -> set(String.valueOf(k), v));
	}

	/**
	 * load properties from a file, see {@link Properties#load(InputStream)} for
	 * the format
	 *
	 * @param filename file
	 * @throws IOException load io exception
	 */
	default void load(String filename) throws IOException {
		load(Path.of(Objects.requireNonNull(filename, "filename can't be null")));
	}

	/**
	 * set an option value
	 *
	 * @param key   key
	 * @param value value
	 */
	void set(String key, String value);

	/**
	 * set a value, check the type of the value to serialize it.
	 *
	 * @param key   key
	 * @param value value
	 */
	default void set(String key, Object value) {
		if (value instanceof RDFFluxStop fs) {
			set(key, fs);
		} else if (value instanceof Path p) {
			set(key, p.toAbsolutePath().toString());
		} else if (value instanceof EnumSet<?> p) {
			set(key, p);
		} else if (value instanceof File f) {
			set(key, f.getAbsolutePath());
		} else if (value instanceof HDTOptions opt) {
			for (Object optKey : opt.getKeys()) {
				String optKeyStr = String.valueOf(optKey);
				set(key + "." + optKeyStr, opt.get(optKeyStr));
			}
		} else {
			set(key, String.valueOf(value));
		}
	}

	/**
	 * set a flux stop value, same as using {@link #set(String, String)} with
	 * {@link RDFFluxStop#asConfig()}
	 *
	 * @param key      key
	 * @param fluxStop value
	 */
	default void set(String key, RDFFluxStop fluxStop) {
		set(key, fluxStop.asConfig());
	}

	/**
	 * set a profiler id
	 *
	 * @param key      key
	 * @param profiler profiler
	 */
	default void set(String key, Profiler profiler) {
		set(key, "!" + profiler.getId());
	}

	/**
	 * set a long value
	 *
	 * @param key   key
	 * @param value value
	 */
	default void setInt(String key, long value) {
		set(key, String.valueOf(value));
	}

	/**
	 * read an option config, format: (key=value)?(;key=value)*
	 *
	 * @param options options
	 */
	default void setOptions(String options) {
		for (String item : options.split(";")) {
			int pos = item.indexOf('=');
			if (pos != -1) {
				String property = item.substring(0, pos);
				String value = item.substring(pos + 1);
				set(property, value);
			}
		}
	}

	/**
	 * add options
	 *
	 * @param options options
	 */
	default void setOptions(Map<?, ?> options) {
		options.forEach((k, v) -> set(String.valueOf(k), v));
	}

	/**
	 * add options from another set
	 *
	 * @param other other set
	 */
	default void setOptions(HDTOptions other) {
		other.getKeys().forEach(key -> set(String.valueOf(key), other.get(String.valueOf(key))));
	}

	/**
	 * add options, each param should be in the format (key, value)*
	 *
	 * @param options options
	 */
	default void setOptions(Object... options) {
		if ((options.length & 1) != 0) {
			throw new IllegalArgumentException("options.length should be even!");
		}

		int len = options.length >> 1;
		for (int i = 0; i < len; i++) {
			String key = String.valueOf(options[(i << 1)]);
			Object value = options[(i << 1) | 1];
			set(key, value);
		}
	}

	/**
	 * Write this options into a config file
	 *
	 * @param file file
	 * @throws IOException io exception
	 */
	default void write(Path file) throws IOException {
		write(file, true);
	}

	/**
	 * Write this options into a config file
	 *
	 * @param file        file
	 * @param withComment write comments
	 * @throws IOException io exception
	 */
	default void write(Path file, boolean withComment) throws IOException {
		try (Writer w = Files.newBufferedWriter(file)) {
			write(w, withComment);
		}
	}

	/**
	 * Write this options into a config file
	 *
	 * @param w           writer
	 * @param withComment write comments
	 * @throws IOException io exception
	 */
	default void write(Writer w, boolean withComment) throws IOException {
		Map<String, HDTOptionsKeys.Option> optionMap = HDTOptionsKeys.getOptionMap();

		for (Object okey : getKeys()) {
			String key = String.valueOf(okey);
			String value = get(key);

			if (withComment) {
				HDTOptionsKeys.Option opt = optionMap.get(key);
				if (opt != null) {
					w.write("# " + opt.getKeyInfo().desc() + "\n# Type: " + opt.getKeyInfo().type().getTitle() + "\n");
				}
			}
			w.write(key + "=" + UnicodeEscape.escapeString(value) + "\n");
		}
	}

	/**
	 * create a new modifiable options with a push top opt, all new set will be
	 * made on top of the previous options, it can be used to overwrite options,
	 * deletion isn't working.
	 *
	 * @return top pushed hdt options
	 */
	default HDTOptions pushTop() {
		HDTOptions top = of();

		return new HDTOptions() {
			@Override
			public void clear() {
				HDTOptions.this.clear();
				top.clear();
			}

			@Override
			public String get(String key) {
				String other = top.get(key);
				if (other != null) {
					return other;
				}
				return HDTOptions.this.get(key);
			}

			@Override
			public void set(String key, String value) {
				top.set(key, value);
			}

			@Override
			public Set<?> getKeys() {
				Set<Object> keys = new HashSet<>();
				keys.addAll(top.getKeys());
				keys.addAll(HDTOptions.this.getKeys());
				return keys;
			}
		};
	}

	/**
	 * create a new modifiable options with a push bottom opt, all new set will
	 * be made on the bottom of the previous options, it can be used to create
	 * default options, deletion isn't working.
	 *
	 * @return top pushed hdt options
	 */
	default HDTOptions pushBottom() {
		HDTOptions bottom = of();

		return new HDTOptions() {
			@Override
			public void clear() {
				HDTOptions.this.clear();
				bottom.clear();
			}

			@Override
			public String get(String key) {
				String other = HDTOptions.this.get(key);
				if (other != null) {
					return other;
				}
				return bottom.get(key);
			}

			@Override
			public void set(String key, String value) {
				bottom.set(key, value);
			}

			@Override
			public Set<?> getKeys() {
				Set<Object> keys = new HashSet<>();
				keys.addAll(bottom.getKeys());
				keys.addAll(HDTOptions.this.getKeys());
				return keys;
			}
		};
	}

	/**
	 * @return a readonly version of these options
	 */
	default HDTOptions readOnly() {
		return new HDTOptions() {
			@Override
			public void clear() {
				throw new IllegalArgumentException("trying to clear a readonly HDTOptions!");
			}

			@Override
			public String get(String key) {
				return HDTOptions.this.get(key);
			}

			@Override
			public void set(String key, String value) {
				throw new IllegalArgumentException("trying to set into a readonly HDTOptions!");
			}

			@Override
			public Set<?> getKeys() {
				return HDTOptions.this.getKeys();
			}
		};
	}

	/**
	 * set enum set, the elements are split using comas
	 *
	 * @param key key
	 * @param set set
	 */
	default void set(String key, EnumSet<?> set) {
		set(key, set.stream().map(Enum::name).collect(Collectors.joining(",")));
	}

	/**
	 * get enum set from value, the elements are split using comas
	 *
	 * @param key key
	 * @param cls enum class
	 * @return enum set
	 * @param <E> enum type
	 */
	default <E extends Enum<E>> EnumSet<E> getEnumSet(String key, Class<E> cls) {
		return getEnumSet(key, cls, EnumSet.noneOf(cls));
	}

	/**
	 * get enum set from value, the elements are split using comas
	 *
	 * @param key          key
	 * @param cls          enum class
	 * @param defaultValue default value
	 * @return enum set
	 * @param <E> enum type
	 */
	default <E extends Enum<E>> EnumSet<E> getEnumSet(String key, Class<E> cls, EnumSet<E> defaultValue) {
		return getEnumSet(key, cls, () -> defaultValue);
	}

	/**
	 * get enum set from value, the elements are split using comas
	 *
	 * @param key          key
	 * @param cls          enum class
	 * @param defaultValue default value supplier
	 * @return enum set
	 * @param <E> enum type
	 */
	default <E extends Enum<E>> EnumSet<E> getEnumSet(String key, Class<E> cls, Supplier<EnumSet<E>> defaultValue) {
		String val = get(key);

		if (val == null || val.isEmpty()) {
			return defaultValue.get();
		}

		EnumSet<E> set = EnumSet.noneOf(cls);

		String[] values = val.split(",");

		mainFor:
		for (String value : values) {
			for (E e : cls.getEnumConstants()) {
				if (e.name().equalsIgnoreCase(value)) {
					set.add(e);
					continue mainFor;
				}
			}
			throw new IllegalArgumentException("Bad option value: " + value);
		}

		return set;
	}

	default HDTOptions getSubOptions(String namespace) {
		HDTOptions that = this;
		return new HDTOptions() {
			@Override
			public void clear() {
				this.getKeys().forEach(k -> {
					String s = k.toString();
					if (!namespace.startsWith(s)) {
						return; // not from our prefix
					}

					that.set(s, "");
				});
			}

			@Override
			public String get(String key) {
				return that.get(namespace + key);
			}

			@Override
			public void set(String key, String value) {
				that.set(namespace + key, value);
			}

			@Override
			public Set<?> getKeys() {
				return that.getKeys().stream().filter(k -> namespace.startsWith(k.toString()))
						.collect(Collectors.toSet());
			}
		};
	}
}
