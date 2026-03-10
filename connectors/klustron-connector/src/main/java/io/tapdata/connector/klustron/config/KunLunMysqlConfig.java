package io.tapdata.connector.klustron.config;

import io.tapdata.connector.mysql.config.MysqlConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/3/4 09:31 Create
 * @description
 */
public class KunLunMysqlConfig extends MysqlConfig {
    List<ComputeNode> computeNode;
    List<StorageNode> storageNode;
    String db;
    String sc;
    Boolean materAble;
    String mateSql;

    @Override
    public MysqlConfig load(Map<String, Object> map) {
        MysqlConfig c = super.load(map);
        //@todo
        c.setPort(computeNode.get(0).getPortMysql());
        if ("postgres".equals(db)) {
            c.setDatabase(sc);
        } else {
            c.setDatabase(String.format("%s.%s", db, sc));
        }
        c.setSchema(getDatabase());
        return c;
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

    public List<StorageNode> getStorageNode() {
        return storageNode;
    }

    public void setStorageNode(List<Object> storageNode) {
        this.storageNode = new ArrayList<>();
        for (Object o : storageNode) {
            if (o instanceof StorageNode) {
                this.storageNode.add((StorageNode) o);
            } else if (o instanceof  Map<?, ?> map) {
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
