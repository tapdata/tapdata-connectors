package io.tapdata.connector.hudi.write.generic;

import io.tapdata.connector.hudi.write.ClientPerformer;
import io.tapdata.connector.hudi.write.generic.entity.KeyEntity;
import io.tapdata.connector.hudi.write.generic.entity.NormalEntity;
import io.tapdata.entity.error.CoreException;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;

import java.util.Map;

import static io.tapdata.base.ConnectorBase.toJson;

public class HoodieRecordGenericStage implements GenericStage<NormalEntity, Map<String, Object>, HoodieRecord<HoodieRecordPayload>>{
    @Override
    public HoodieRecord<HoodieRecordPayload> generic(Map<String, Object> fromValue, NormalEntity entity) {
        try {
            ClientPerformer clientPerformer = entity.getClientEntity();
            GenericRecord genericRecord = GenericMapToRecord.singleton().generic(fromValue, entity);
            HoodieKey hoodieKey = GenericHoodieKey.singleton().generic(genericRecord,  new KeyEntity().withKeyNames(clientPerformer.getPrimaryKeys()).withPartitionKeys(clientPerformer.getPartitionKeys()));
            HoodieRecordPayload<HoodieAvroPayload> payload = new org.apache.hudi.common.model.HoodieAvroPayload(Option.of(genericRecord));
            return new HoodieAvroRecord<>(hoodieKey, payload);
        } catch (Exception e) {
            throw new CoreException(e, String.format("Fail to generic hudi record from map, value: %s, message: %s", toJson(fromValue), e.getMessage()));
        }
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
