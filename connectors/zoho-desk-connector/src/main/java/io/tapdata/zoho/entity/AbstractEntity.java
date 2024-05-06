package io.tapdata.zoho.entity;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractEntity<K,V> {
    protected Map<K,V> entity;

    protected AbstractEntity(){
        this.entity = new HashMap<>();
    }

}
