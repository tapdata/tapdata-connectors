package io.tapdata.connector.hudi.write.generic;

import io.tapdata.connector.hudi.write.ClientEntity;
import io.tapdata.connector.hudi.write.generic.entity.KeyEntity;
import io.tapdata.connector.hudi.write.generic.entity.NormalEntity;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.util.Option;

import java.util.Map;

public class HoodieRecordGenericStage implements GenericStage<NormalEntity, Map<String, Object>, HoodieRecord<HoodieRecordPayload>>{
    @Override
    public HoodieRecord<HoodieRecordPayload> generic(Map<String, Object> fromValue, NormalEntity entity) {
        ClientEntity clientEntity = entity.getClientEntity();
        GenericRecord genericRecord = GenericMapToRecord.singleton().generic(fromValue, entity);
        HoodieKey hoodieKey = GenericHoodieKey.singleton().generic(genericRecord,  new KeyEntity().withKeyNames(clientEntity.getPrimaryKeys()).withPartitionKeys(clientEntity.getPartitionKeys()));
        HoodieRecordPayload<HoodieAvroPayload> payload = new org.apache.hudi.common.model.HoodieAvroPayload(Option.of(genericRecord));
        return new HoodieAvroRecord<>(hoodieKey, payload);
    }

    private static HoodieRecordGenericStage singleton;
    public static HoodieRecordGenericStage singleton() {
        if (null == HoodieRecordGenericStage.singleton) {
            synchronized (HoodieRecordGenericStage.class) {
                if (null == HoodieRecordGenericStage.singleton) {
                    HoodieRecordGenericStage.singleton = new HoodieRecordGenericStage();
                }
            }
        }
        return HoodieRecordGenericStage.singleton;
    }
    private HoodieRecordGenericStage() {
    }
}
