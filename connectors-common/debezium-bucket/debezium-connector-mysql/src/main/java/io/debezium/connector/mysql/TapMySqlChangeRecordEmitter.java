package io.debezium.connector.mysql;

import io.debezium.data.Envelope;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableSchema;
import io.debezium.type.TapIllegalDate;
import io.debezium.util.Clock;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TapMySqlChangeRecordEmitter extends MySqlChangeRecordEmitter{
    private Map<TapIllegalDate, Integer> illegalDateIntegerMap;
    public TapMySqlChangeRecordEmitter(OffsetContext offset, Clock clock, Envelope.Operation operation, Serializable[] before, Serializable[] after) {
        super(offset, clock, operation, before, after);

        this.illegalDateIntegerMap = new HashMap<>();
        if (null != before) {
            int beforeIndex = 0;
            for (Serializable serializable : before) {
                if (serializable instanceof TapIllegalDate){
                    if (Integer.class == ((TapIllegalDate) serializable).getOriginDateType()){
                        serializable = Integer.MIN_VALUE;
                    }else if (String.class == ((TapIllegalDate) serializable).getOriginDateType()){
                        serializable = new String();
                    }else {
                        serializable = Long.MIN_VALUE;
                    }
                    before[beforeIndex] = serializable;
                }
                beforeIndex++;
            }
        }
        if (null != after){
            int afterIndex = 0;
            for (Serializable serializable : after) {
                if (serializable instanceof TapIllegalDate){
                    illegalDateIntegerMap.put((TapIllegalDate) serializable, afterIndex);
                    if ("integer".equals(((TapIllegalDate) serializable).getOriginDateType())){
                        serializable = Integer.MIN_VALUE;
                    }else {
                        serializable = Long.MIN_VALUE;
                    }
                    after[afterIndex] = serializable;
                }
                afterIndex++;
            }
        }
    }

    public Map<TapIllegalDate, Integer> getIllegalDateIntegerMap() {
        return illegalDateIntegerMap;
    }
    private Map<TapIllegalDate, Integer> illegalDateSchemaMap = new HashMap<>();

    @Override
    protected Map<Boolean,Struct> illegalValueFromMap(TableSchema tableSchema) {
        Map<Boolean,Struct> res = new HashMap();
        Schema schema = tableSchema.valueSchema();
        SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        AtomicInteger index = new AtomicInteger(0);
        illegalDateIntegerMap.forEach((k,v)->{
            for (Field field : schema.fields()) {
                if (v == field.index()){
                    schemaBuilder.field(field.name(),Schema.BYTES_SCHEMA);
                    illegalDateSchemaMap.put(k,index.get());
                    index.addAndGet(1);
                }
            }
        });
        Schema invalidSchema = schemaBuilder.build();
        Struct invalidStruct = new Struct(invalidSchema);
        illegalDateSchemaMap.forEach((k,v)->{
            for (Field field : invalidSchema.fields()) {
                if (v == field.index()){
                    try {
                        invalidStruct.put(field,k.illegalDateToByte(k));
                        res.put(true,invalidStruct);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });

        return res;
    }

}
