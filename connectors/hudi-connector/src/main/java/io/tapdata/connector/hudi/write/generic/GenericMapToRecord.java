package io.tapdata.connector.hudi.write.generic;

import io.tapdata.connector.hudi.write.ClientEntity;
import io.tapdata.connector.hudi.write.generic.entity.NormalEntity;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.Map;

public class GenericMapToRecord implements GenericStage<NormalEntity, Map<String, Object>, GenericRecord> {
    @Override
    public GenericRecord generic(Map<String, Object> fromValue, NormalEntity genericParam) {
        if (null == fromValue) return null;
        ClientEntity clientEntity = genericParam.getClientEntity();
        Schema schema = clientEntity.getSchema();
        GenericRecord genericRecord = new GenericData.Record(schema);
        if (fromValue.isEmpty()) return genericRecord;
        fromValue.forEach(genericRecord::put);
        return genericRecord;
    }

    private static GenericMapToRecord singleton;
    public static GenericMapToRecord singleton() {
        if (null == GenericMapToRecord.singleton) {
            synchronized (GenericMapToRecord.class) {
                if (null == GenericMapToRecord.singleton) {
                    GenericMapToRecord.singleton = new GenericMapToRecord();
                }
            }
        }
        return GenericMapToRecord.singleton;
    }
    private GenericMapToRecord() {
    }
}
