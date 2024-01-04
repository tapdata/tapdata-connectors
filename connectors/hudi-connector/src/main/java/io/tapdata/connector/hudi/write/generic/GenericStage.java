package io.tapdata.connector.hudi.write.generic;

import io.tapdata.connector.hudi.write.ClientEntity;
import io.tapdata.connector.hudi.write.generic.entity.Entity;
import io.tapdata.entity.schema.TapTable;

public interface GenericStage<Param extends Entity, From, To> {
    public To generic(From fromValue, Param genericParam);
}
