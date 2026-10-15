package io.tapdata.connector.hudi.write.generic;

import io.tapdata.connector.hudi.write.generic.entity.Entity;

public interface GenericStage<Param extends Entity, From, To> {
    public To generic(From fromValue, Param genericParam);
}
