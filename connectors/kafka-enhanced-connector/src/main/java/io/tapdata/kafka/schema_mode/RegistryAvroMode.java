package io.tapdata.kafka.schema_mode;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kafka.AbsSchemaMode;
import io.tapdata.kafka.IKafkaService;
import io.tapdata.kafka.constants.KafkaSchemaMode;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class RegistryAvroMode extends AbsSchemaMode {

    public RegistryAvroMode(IKafkaService kafkaService) {
        super(KafkaSchemaMode.REGISTRY_AVRO, kafkaService);
    }

    @Override
    public void discoverSchema(IKafkaService kafkaService, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) {

    }

    @Override
    public TapEvent toTapEvent(ConsumerRecord<?, ?> consumerRecord) {
        return null;
    }

    @Override
    public List<ProducerRecord<Object, Object>> fromTapEvent(TapTable tapTable, TapEvent tapEvent) {
        Map<String, Object> data;
        if (tapEvent instanceof TapInsertRecordEvent) {
            data = ((TapInsertRecordEvent) tapEvent).getAfter();
        } else if (tapEvent instanceof TapUpdateRecordEvent) {
            data = ((TapUpdateRecordEvent) tapEvent).getAfter();
        } else if (tapEvent instanceof TapDeleteRecordEvent) {
            data = ((TapDeleteRecordEvent) tapEvent).getBefore();
        } else {
            data = new HashMap<>();
        }

        SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(tapTable.getId());
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();

        final Map<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        for (final String columnName : nameFieldMap.keySet()) {
            final String columnType = nameFieldMap.get(columnName).getDataType();
            if (StringUtils.isBlank(columnType)) {
                continue;
            }
            // 根据列的类型映射为 Avro 模式的类型
            Schema.Field field = createAvroField(columnName, columnType);
            fieldAssembler.name(columnName).type(field.schema()).noDefault();
        }
        Schema.Parser parser = new Schema.Parser();
        Schema avroSchema = parser.parse(fieldAssembler.endRecord().toString());
        GenericRecord record = new GenericData.Record(avroSchema);
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();
            record.put(fieldName, value);
        }

        ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(
                tapTable.getId(),
                record
        );
        return List.of(producerRecord);
    }

    private static Schema.Field createAvroField(String columnName, String columnType) {
        Schema avroType;
        switch (columnType) {
            case "BOOLEAN":
                avroType = SchemaBuilder.builder().booleanType();
                break;
            case "NUMBER":
                avroType = SchemaBuilder.builder().doubleType();
                break;
            case "INTEGER":
                avroType = SchemaBuilder.builder().longType();
                break;
            case "STRING":
            case "ARRAY":
            case "TEXT":
            default:
                avroType = SchemaBuilder.builder().stringType();
                break;
        }

        return new Schema.Field(columnName, avroType, null, null);
    }

    @Override
    public void queryByAdvanceFilter(TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) {

    }
}
