package io.tapdata.zoho.entity;

import java.util.Map;
import java.util.Objects;

public class HttpNormalEntity extends AbstractEntity<String, String> {

    public static HttpNormalEntity create(){
        return new HttpNormalEntity();
    }

    public static HttpNormalEntity create(Map<String, String> entity) {
        HttpNormalEntity httpEntity = new HttpNormalEntity();
        if (Objects.nonNull(entity)) {
            httpEntity.addAll(entity);
        }
        return httpEntity;
    }

    public HttpNormalEntity addAll(Map<String, String> entity) {
        this.entity.putAll(entity);
        return this;
    }

    public HttpNormalEntity build(String key, String value) {
        this.entity.put(key,value);
        return this;
    }

    public HttpNormalEntity remove(String key) {
        this.entity.remove(key);
        return this;
    }

    public HttpNormalEntity build(Map.Entry<String, String> entry) {
        this.entity.put(entry.getKey(),entry.getValue());
        return this;
    }

    public Map<String, String> entity() {
        return this.entity;
    }

    public String get(String key) {
        return this.entity.get(key);
    }

    public Map<String, String> getEntity() {
        return entity;
    }

    public void setEntity(Map<String, String> entity) {
        this.entity = entity;
    }
}