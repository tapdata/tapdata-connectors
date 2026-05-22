package io.tapdata.perftest.connector.paimon.support;

import io.tapdata.entity.utils.cache.KVMap;

public class SimpleKVMap<T> extends SimpleKVReadOnlyMap<T> implements KVMap<T> {

    @Override
    public void init(String mapKey, Class<T> valueClass) {}

    @Override
    public void put(String key, T value) {
        data.put(key, value);
    }

    @Override
    public T putIfAbsent(String key, T value) {
        return data.putIfAbsent(key, value);
    }

    @Override
    public T remove(String key) {
        return data.remove(key);
    }

    @Override
    public void clear() {
        data.clear();
    }

    @Override
    public void reset() {
        data.clear();
    }
}
