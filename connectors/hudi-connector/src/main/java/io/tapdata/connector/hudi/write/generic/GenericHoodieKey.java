package io.tapdata.connector.hudi.write.generic;

import io.tapdata.connector.hudi.write.generic.entity.KeyEntity;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.keygen.CustomAvroKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

public class GenericHoodieKey implements GenericStage<KeyEntity, GenericRecord, HoodieKey> {
    private static GenericHoodieKey singleton;
    public static GenericHoodieKey singleton() {
        if (null == GenericHoodieKey.singleton) {
            synchronized (GenericHoodieKey.class) {
                if (null == GenericHoodieKey.singleton) {
                    GenericHoodieKey.singleton = new GenericHoodieKey();
                }
            }
        }
        return GenericHoodieKey.singleton;
    }
    private GenericHoodieKey() {
    }

    @Override
    public HoodieKey generic(GenericRecord fromValue, KeyEntity genericParam) {
        TypedProperties props = new TypedProperties();
        props.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), StringUtils.join(genericParam.getKeyNames(), ","));
        props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), StringUtils.join(genericParam.getPartitionKeys(), ","));
        CustomAvroKeyGenerator keyGenerator = new CustomAvroKeyGenerator(props);
        return keyGenerator.getKey(fromValue);
    }
}
