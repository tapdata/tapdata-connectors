package io.tapdata.connector.hudi.write.generic.entity;

import java.util.Set;

public class KeyEntity extends Entity {
    Set<String> keyNames;
    Set<String> partitionKeys;
    public KeyEntity withKeyNames(Set<String> keyNames) {
        this.keyNames = keyNames;
        return this;
    }
    public KeyEntity withPartitionKeys(Set<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
        return this;
    }

    public Set<String> getKeyNames() {
        return keyNames;
    }

    public Set<String> getPartitionKeys() {
        return partitionKeys;
    }
}