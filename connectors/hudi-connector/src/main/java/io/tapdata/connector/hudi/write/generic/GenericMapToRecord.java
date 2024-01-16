package io.tapdata.connector.hudi.write.generic;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.logger.Log;
import io.tapdata.connector.hudi.write.ClientPerformer;
import io.tapdata.connector.hudi.write.generic.entity.NormalEntity;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Map;
import java.util.Optional;

public class GenericMapToRecord implements GenericStage<NormalEntity, Map<String, Object>, GenericRecord> {
    @Override
    public GenericRecord generic(Map<String, Object> fromValue, NormalEntity genericParam) {
        if (null == fromValue) return null;
        ClientPerformer clientPerformer = genericParam.getClientEntity();
        Schema schema = clientPerformer.getSchema();
        GenericRecord genericRecord = new GenericData.Record(schema);
        if (fromValue.isEmpty()) return genericRecord;
        fromValue.forEach((key,value) -> {
            if (value instanceof Short) {
                genericRecord.put(key, ((Short) value).intValue());
            } else if (value instanceof BigInteger) {
                genericRecord.put(key, ((BigInteger) value).longValue());
            } else if (value instanceof BigDecimal) {
                Object decimal = handelBigDecimal(key, (BigDecimal) value, clientPerformer.getTapTable(), clientPerformer.getLog());
                genericRecord.put(key, decimal);
            } else {
                genericRecord.put(key, value);
            }
        });
        return genericRecord;
    }
    private Object handelBigDecimal(String key, BigDecimal value, TapTable table, Log log) {
        if (null == table || null == table.getNameFieldMap()) return value;
        TapField field = table.getNameFieldMap().get(key);
        if (null == field || null == field.getTapType()) return value;
        TapType tapType = field.getTapType();
        if (tapType instanceof TapNumber) {
            int scale = Optional.of(((TapNumber) tapType).getScale()).orElse(0);
            int precision = Optional.ofNullable(((TapNumber) tapType).getPrecision()).orElse(0);
            if (value.scale() != scale || value.precision() <= 0) {
                log.warn("Find an BigDecimal({},{}), but schema data type of field [{}] in table [{}] is decimal({}, {}), scale not equals. BigDecimal value: {}",
                        value.precision(),
                        value.scale(),
                        key,
                        table.getId(),
                        precision,
                        scale, value.toString());
            }
            if (value.scale() != scale) {
                return new BigDecimal(value.setScale(scale, RoundingMode.UNNECESSARY).toString());
            }
            if (value.precision() <= 0) {
                return new BigDecimal(value.toString());
            }
        }
        return value;
    }

    private static volatile GenericMapToRecord singleton;
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
