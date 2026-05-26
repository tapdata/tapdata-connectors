package io.tapdata.perftest.connector.paimon.support;

import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleKVReadOnlyMap<T> implements KVReadOnlyMap<T> {

    protected final Map<String, T> data = new ConcurrentHashMap<>();

    public void put(String key, T value) {
        data.put(key, value);
    }

    @Override
    public T get(String key) {
        return data.get(key);
    }

    @Override
    public Iterator<Entry<T>> iterator() {
        java.util.Iterator<Map.Entry<String, T>> it = data.entrySet().iterator();
        return new Iterator<Entry<T>>() {
            @Override public boolean hasNext() { return it.hasNext(); }
            @Override public Entry<T> next() {
                Map.Entry<String, T> e = it.next();
                return new Entry<T>() {
                    @Override public String getKey()   { return e.getKey(); }
                    @Override public T      getValue() { return e.getValue(); }
                };
            }
        };
    }
}
