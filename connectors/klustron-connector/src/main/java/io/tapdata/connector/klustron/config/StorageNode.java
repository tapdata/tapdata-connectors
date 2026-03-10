package io.tapdata.connector.klustron.config;

import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;

import java.io.Serializable;
import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/3/5 16:58 Create
 * @description
 */
public class StorageNode implements Serializable {
    protected static final BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class);
    String host;
    Integer port;
    String username;
    String password;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public StorageNode load(Map<?, ?> o) {
        assert beanUtils != null;
        beanUtils.mapToBean((Map<String, Object>) o, this);
        return this;
    }
}
