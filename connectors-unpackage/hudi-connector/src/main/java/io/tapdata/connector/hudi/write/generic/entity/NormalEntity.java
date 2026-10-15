package io.tapdata.connector.hudi.write.generic.entity;

import io.tapdata.connector.hudi.write.ClientPerformer;
import io.tapdata.entity.schema.TapTable;

public class NormalEntity extends Entity {
    TapTable tapTable;
    ClientPerformer clientPerformer;
    public NormalEntity withTapTable(TapTable tapTable) {
        this.tapTable = tapTable;
        return this;
    }
    public NormalEntity withClientEntity(ClientPerformer clientPerformer) {
        this.clientPerformer = clientPerformer;
        return this;
    }

    public TapTable getTapTable() {
        return tapTable;
    }

    public ClientPerformer getClientEntity() {
            return clientPerformer;
        }
}