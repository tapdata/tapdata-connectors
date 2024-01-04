package io.tapdata.connector.hudi.write.generic.entity;

import io.tapdata.connector.hudi.write.ClientEntity;
import io.tapdata.entity.schema.TapTable;

public class NormalEntity extends Entity {
    TapTable tapTable;
    ClientEntity clientEntity;
    public NormalEntity withTapTable(TapTable tapTable) {
        this.tapTable = tapTable;
        return this;
    }
    public NormalEntity withClientEntity(ClientEntity clientEntity) {
        this.clientEntity = clientEntity;
        return this;
    }

    public TapTable getTapTable() {
        return tapTable;
    }

    public ClientEntity getClientEntity() {
            return clientEntity;
        }
}