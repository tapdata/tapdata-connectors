package io.tapdata.connector.klustron.config;

import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;

import java.io.Serializable;
import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/3/5 16:57 Create
 * @description
 */
public class ComputeNode implements Serializable {
    protected static final BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class);
    String host;
    Integer portMysql;
    Integer portPg;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPortMysql() {
        return portMysql;
    }

    public void setPortMysql(Integer portMysql) {
        this.portMysql = portMysql;
    }

    public Integer getPortPg() {
        return portPg;
    }

    public void setPortPg(Integer portPg) {
        this.portPg = portPg;
    }

    public ComputeNode load(Map<?, ?> o) {
        assert beanUtils != null;
        beanUtils.mapToBean((Map<String, Object>) o, this);
        return this;
    }
}
