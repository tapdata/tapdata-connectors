package io.tapdata.connector.elasticsearch;

import io.tapdata.connector.elasticsearch.cons.FieldsMappingMode;
import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;

import java.util.Map;

public class ElasticsearchConfig {

    private static final BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class); //bean util

    private String host;
    private int port;
    private String user;
    private String password;
    private String dateFormat = "yyyy-MM-dd";
    private String datetimeFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private String timeFormat = "HH:mm:ss";
    private Integer shardsNumber = 1;
    private Integer replicasNumber = 1;
    private Integer fieldsLimit = 1000;
    private String fieldsMappingMode;

    public String getDatetimeFormat() {
        return datetimeFormat;
    }

    public void setDatetimeFormat(String datetimeFormat) {
        this.datetimeFormat = datetimeFormat;
    }

    public String getTimeFormat() {
        return timeFormat;
    }

    public void setTimeFormat(String timeFormat) {
        this.timeFormat = timeFormat;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    public ElasticsearchConfig load(Map<String, Object> map)  {
        return beanUtils.mapToBean(map, this);
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Integer getShardsNumber() {
        return shardsNumber;
    }

    public void setShardsNumber(Integer shardsNumber) {
        this.shardsNumber = shardsNumber;
    }

    public Integer getReplicasNumber() {
        return replicasNumber;
    }

    public void setReplicasNumber(Integer replicasNumber) {
        this.replicasNumber = replicasNumber;
    }

    public Integer getFieldsLimit() {
        return fieldsLimit;
    }

    public void setFieldsLimit(Integer fieldsLimit) {
        this.fieldsLimit = fieldsLimit;
    }

    public FieldsMappingMode getFieldsMappingModeEnum() {
        return FieldsMappingMode.fromString(fieldsMappingMode);
    }

    public String getFieldsMappingMode() {
        return fieldsMappingMode;
    }

    public void setFieldsMappingMode(String fieldsMappingMode) {
        this.fieldsMappingMode = fieldsMappingMode;
    }
}
