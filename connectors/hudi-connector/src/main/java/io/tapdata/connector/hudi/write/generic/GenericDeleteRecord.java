package io.tapdata.connector.hudi.write.generic;

import io.tapdata.connector.hudi.write.ClientPerformer;
import io.tapdata.connector.hudi.write.generic.entity.KeyEntity;
import io.tapdata.connector.hudi.write.generic.entity.NormalEntity;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.model.HoodieKey;

import java.util.Map;

public class GenericDeleteRecord implements GenericStage<NormalEntity, Map<String, Object>, HoodieKey> {
    private static GenericDeleteRecord singleton;
    public static GenericDeleteRecord singleton() {
        if (null == GenericDeleteRecord.singleton) {
            synchronized (GenericDeleteRecord.class) {
                if (null == GenericDeleteRecord.singleton) {
                    GenericDeleteRecord.singleton = new GenericDeleteRecord();
                }
            }
        }
        return GenericDeleteRecord.singleton;
    }
    private GenericDeleteRecord() {
    }
    @Override
    public HoodieKey generic(Map<String, Object> fromValue, NormalEntity genericParam) {
        ClientPerformer clientPerformer = genericParam.getClientEntity();
        GenericRecord genericRecord = GenericMapToRecord.singleton().generic(fromValue, genericParam);
        return GenericHoodieKey.singleton().generic(
                genericRecord,
                new KeyEntity().withKeyNames(clientPerformer.getPrimaryKeys()).withPartitionKeys(clientPerformer.getPartitionKeys()));
    }
}
