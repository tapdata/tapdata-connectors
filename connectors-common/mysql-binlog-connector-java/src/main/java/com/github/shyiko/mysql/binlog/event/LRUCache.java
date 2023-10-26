package com.github.shyiko.mysql.binlog.event;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<K,V> extends LinkedHashMap<K,V> {
    private int maxSize;

    // and other constructors for load factor and hashtable capacity
    public LRUCache(int initialCapacity, float loadFactor, int maxSize) {
        super(initialCapacity, loadFactor, true);
        this.maxSize = maxSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > maxSize;
    }
}
