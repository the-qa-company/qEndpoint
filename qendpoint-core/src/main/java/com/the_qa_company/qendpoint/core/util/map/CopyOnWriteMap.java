package com.the_qa_company.qendpoint.core.util.map;

import java.io.Serial;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Copy on write map
 *
 * @param <K> key type
 * @param <V> value type
 * @author Antoine Willerval
 */
public class CopyOnWriteMap<K, V> implements Map<K, V>, Serializable {

	@Serial
	private static final long serialVersionUID = -3127117388123088572L;

	private Map<K, V> wrapper;
	private boolean write;

	public CopyOnWriteMap(Map<K, V> wrapper) {
		this.wrapper = wrapper;
	}

	private Map<K, V> update() {
		if (!write) {
			wrapper = new HashMap<>(wrapper);
			write = true;
		}
		return wrapper;
	}

	@Override
	public int size() {
		return wrapper.size();
	}

	@Override
	public boolean isEmpty() {
		return wrapper.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return wrapper.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return wrapper.containsValue(value);
	}

	@Override
	public V get(Object key) {
		return wrapper.get(key);
	}

	@Override
	public V put(K key, V value) {
		return update().put(key, value);
	}

	@Override
	public V remove(Object key) {
		return update().remove(key);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		update().putAll(m);
	}

	@Override
	public void clear() {
		update().clear();
	}

	@Override
	public Set<K> keySet() {
		return wrapper.keySet();
	}

	@Override
	public Collection<V> values() {
		return wrapper.values();
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		return wrapper.entrySet();
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof Map<?, ?> map)) {
			return false;
		}
		return map.equals(o);
	}

	@Override
	public int hashCode() {
		return wrapper.hashCode();
	}

	@Override
	public V getOrDefault(Object key, V defaultValue) {
		return wrapper.getOrDefault(key, defaultValue);
	}

	@Override
	public void forEach(BiConsumer<? super K, ? super V> action) {
		wrapper.forEach(action);
	}

	@Override
	public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
		update().replaceAll(function);
	}

	@Override
	public V putIfAbsent(K key, V value) {
		return update().putIfAbsent(key, value);
	}

	@Override
	public boolean remove(Object key, Object value) {
		return update().remove(key, value);
	}

	@Override
	public boolean replace(K key, V oldValue, V newValue) {
		return update().replace(key, oldValue, newValue);
	}

	@Override
	public V replace(K key, V value) {
		return update().replace(key, value);
	}

	@Override
	public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
		return update().computeIfAbsent(key, mappingFunction);
	}

	@Override
	public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
		return update().computeIfPresent(key, remappingFunction);
	}

	@Override
	public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
		return update().compute(key, remappingFunction);
	}

	@Override
	public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
		return update().merge(key, value, remappingFunction);
	}
}
