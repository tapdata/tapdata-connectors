package io.tapdata.zoho.entity;

import java.util.Map;
import java.util.Objects;

public class HttpEntity extends AbstractEntity<String, Object> {

    public static HttpEntity create(){
        return new HttpEntity();
    }

    public static HttpEntity create(Map<String, Object> entity) {
        HttpEntity httpEntity = new HttpEntity();
        if (Objects.nonNull(entity)) {
            httpEntity.addAll(entity);
        }
        return httpEntity;
    }

    public HttpEntity addAll(Map<String, Object> entity) {
        this.entity.putAll(entity);
        return this;
    }

    public HttpEntity build(String key, Object value) {
        this.entity.put(key,value);
        return this;
    }

    public HttpEntity remove(String key) {
        this.entity.remove(key);
        return this;
    }

    public HttpEntity build(Map.Entry<String, Object> entry) {
        this.entity.put(entry.getKey(),entry.getValue());
        return this;
    }

    public Map<String, Object> entity() {
        return this.entity;
    }

    public Object get(String key) {
        return this.entity.get(key);
    }

    public Map<String, Object> getEntity() {
        return entity;
    }

    public void setEntity(Map<String, Object> entity) {
        this.entity = entity;
    }
}