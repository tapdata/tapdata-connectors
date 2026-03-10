package io.tapdata.connector.klustron.config;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.connector.postgres.config.PostgresConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/3/4 14:05 Create
 * @description
 */
public class KunLunPgConfig extends PostgresConfig {
    List<ComputeNode> computeNode;
    List<StorageNode> storageNode;
    String username;
    Boolean materAble;
    String mateSql;

    String db;
    String sc;

    @Override
    public CommonDbConfig load(Map<String, Object> map) {
        CommonDbConfig c = super.load(map);
        c.setPort(computeNode.get(0).getPortPg());
        c.setDatabase(db);
        c.setSchema(sc);
        return c;
    }


    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getSc() {
        return sc;
    }

    public void setSc(String sc) {
        this.sc = sc;
    }

    public List<ComputeNode> getComputeNode() {
        return computeNode;
    }

    public List<StorageNode> getStorageNode() {
        return storageNode;
    }

    public void setComputeNode(List<Object> computeNode) {
        this.computeNode = new ArrayList<>();
        for (Object o : computeNode) {
            if (o instanceof ComputeNode) {
                this.computeNode.add((ComputeNode) o);
            } else if (o instanceof Map<?, ?> map) {
                this.computeNode.add(new ComputeNode().load(map));
            }
        }
    }

    public void setStorageNode(List<Object> storageNode) {
        this.storageNode = new ArrayList<>();
        for (Object o : storageNode) {
            if (o instanceof StorageNode) {
                this.storageNode.add((StorageNode) o);
            } else if (o instanceof Map<?, ?> map) {
                this.storageNode.add(new StorageNode().load(map));
            }
        }
    }

    public String getMateSql() {
        return mateSql;
    }

    public void setMateSql(String mateSql) {
        this.mateSql = mateSql;
    }

    public Boolean getMaterAble() {
        return materAble;
    }

    public void setMaterAble(Boolean materAble) {
        this.materAble = materAble;
    }
}
