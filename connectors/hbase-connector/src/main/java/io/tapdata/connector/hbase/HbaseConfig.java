package io.tapdata.connector.hbase;

import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;

import java.util.Map;

public class HbaseConfig {

    private static final BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class);

    private String zookeeperQuorum;
    private String zookeeperParent = "/hbase";
    private String user;
    private String password;
    private int scanCaching = 100;
    private int scanBatch = 100;
    private int writeBatchSize = 1000;

    public HbaseConfig load(Map<String, Object> map) {
        beanUtils.mapToBean(map, this);
        return this;
    }

    public String getZookeeperQuorum() {
        return zookeeperQuorum;
    }

    public void setZookeeperQuorum(String zookeeperQuorum) {
        this.zookeeperQuorum = zookeeperQuorum;
    }

    public String getZookeeperParent() {
        return zookeeperParent;
    }

    public void setZookeeperParent(String zookeeperParent) {
        this.zookeeperParent = zookeeperParent;
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

    public int getScanCaching() {
        return scanCaching;
    }

    public void setScanCaching(int scanCaching) {
        this.scanCaching = scanCaching;
    }

    public int getScanBatch() {
        return scanBatch;
    }

    public void setScanBatch(int scanBatch) {
        this.scanBatch = scanBatch;
    }

    public int getWriteBatchSize() {
        return writeBatchSize;
    }

    public void setWriteBatchSize(int writeBatchSize) {
        this.writeBatchSize = writeBatchSize;
    }
}
