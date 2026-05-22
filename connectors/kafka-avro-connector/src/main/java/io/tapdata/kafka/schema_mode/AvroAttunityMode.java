package io.tapdata.kafka.schema_mode;

import io.tapdata.constant.DMLType;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kafka.IKafkaService;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.tapdata.constant.DMLType.DELETE;
import static io.tapdata.constant.DMLType.INSERT;
import static io.tapdata.constant.DMLType.UPDATE;

/**
 * 以 Attunity / Qlik Replicate 的 Avro 信封结构包裹父类生成的表 schema：
 * <pre>
 * {
 *   "type": "record",
 *   "name": "&lt;tableId&gt;",
 *   "fields": [
 *     {"name": "Data",       "type": ["null", "&lt;tableId&gt;_Data"], "default": null},
 *     {"name": "BeforeData", "type": ["null", "&lt;tableId&gt;_Data"], "default": null},
 *     {"name": "headers",    "type": ["null", "&lt;tableId&gt;_Headers"], "default": null}
 *   ]
 * }
 * </pre>
 * 其中 {@code <tableId>_Data} 复用父类按列生成的 record，BeforeData 通过名字引用同一份。
 */
public class AvroAttunityMode extends AvroEnhanceMode {

    private static final String DATA_RECORD = "Data";
    private static final String HEADERS_RECORD = "Headers";

    public AvroAttunityMode(IKafkaService kafkaService) {
        super(kafkaService);
    }

    /**
     * 覆写 2 参版本，DML 路径 (1 参委托过来) 与 DDL 演进路径 (line 789, 直接调 2 参) 同时进入此处，
     * 保证 Schema Registry 中注册的与运行时写入的 envelope schema 一致。
     */
    @Override
    protected Schema buildAvroSchemaFromTable(TapTable tapTable, Map<String, Set<String>> aliasMap) {
        Schema inner = super.buildAvroSchemaFromTable(tapTable, aliasMap);
        // 外层 envelope 与 inner 同名会触发 Avro 命名冲突，重命名 inner 为 <tableId>_Data
        Schema dataRecord = renameRecord(inner, DATA_RECORD, inner.getNamespace());
        Schema headersRecord = buildHeadersSchema(HEADERS_RECORD, inner.getNamespace());

        Schema envelope = Schema.createRecord(tapTable.getId(), null, inner.getNamespace(), false);
        List<Schema.Field> fields = new ArrayList<>(3);
        fields.add(nullableField(kafkaService.getConfig().getNodeAttunityDataName(), dataRecord));
        fields.add(nullableField(kafkaService.getConfig().getNodeAttunityBeforeDataName(), dataRecord));
        fields.add(nullableField(kafkaService.getConfig().getNodeAttunityHeadersName(), headersRecord));
        envelope.setFields(fields);
        return envelope;
    }

    /**
     * 按 Attunity 语义拆分 before / after 并装进 envelope：
     * <ul>
     *     <li>INSERT: Data = after, BeforeData = null</li>
     *     <li>UPDATE: Data = after, BeforeData = before</li>
     *     <li>DELETE: Data = null,  BeforeData = before</li>
     * </ul>
     */
    @Override
    public List<ProducerRecord<Object, Object>> fromTapEvent(TapTable tapTable, TapEvent tapEvent) {
        Map<String, Object> after = null;
        Map<String, Object> before = null;
        DMLType op;
        if (tapEvent instanceof TapInsertRecordEvent) {
            after = ((TapInsertRecordEvent) tapEvent).getAfter();
            op = INSERT;
        } else if (tapEvent instanceof TapUpdateRecordEvent) {
            after = ((TapUpdateRecordEvent) tapEvent).getAfter();
            before = ((TapUpdateRecordEvent) tapEvent).getBefore();
            op = UPDATE;
        } else if (tapEvent instanceof TapDeleteRecordEvent) {
            before = ((TapDeleteRecordEvent) tapEvent).getBefore();
            op = DELETE;
        } else {
            return Collections.emptyList();
        }

        Schema envelope = buildAvroSchemaFromTable(tapTable);
        // envelope 的 Data / BeforeData 字段都是 ["null", dataRecord]，索引 1 即 dataRecord
        Schema dataRecord = envelope.getField(kafkaService.getConfig().getNodeAttunityDataName()).schema().getTypes().get(1);
        Schema headersRecord = envelope.getField(kafkaService.getConfig().getNodeAttunityHeadersName()).schema().getTypes().get(1);

        GenericRecord envelopeRec = new GenericData.Record(envelope);
        if (after != null && !after.isEmpty()) {
            envelopeRec.put(kafkaService.getConfig().getNodeAttunityDataName(), buildDataRecord(dataRecord, after, tapTable));
        }
        if (before != null && !before.isEmpty()) {
            envelopeRec.put(kafkaService.getConfig().getNodeAttunityBeforeDataName(), buildDataRecord(dataRecord, before, tapTable));
        }
        envelopeRec.put(kafkaService.getConfig().getNodeAttunityHeadersName(), buildHeadersRecord(headersRecord, op, tapEvent));

        // key / partition 路由仍按业务数据走：INSERT/UPDATE 用 after，DELETE 用 before
        Map<String, Object> routing = after != null ? after : (before != null ? before : new HashMap<>());
        ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(
                topic(tapTable, tapEvent),
                computePartition(createKafkaKey(routing, tapTable), kafkaService.getConfig().getNodePartitionSize()),
                tapEvent.getTime(),
                createKafkaKeyValueMap(routing, tapTable),
                envelopeRec,
                new RecordHeaders().add("op", op.name().getBytes()));
        return List.of(producerRecord);
    }

    private GenericRecord buildDataRecord(Schema dataSchema, Map<String, Object> data, TapTable tapTable) {
        Map<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        GenericRecord rec = new GenericData.Record(dataSchema);
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String fieldName = entry.getKey();
            if (dataSchema.getField(fieldName) == null) {
                continue;
            }
            TapField tapField = nameFieldMap == null ? null : nameFieldMap.get(fieldName);
            if (tapField != null) {
                // 复用父类 AvroEnhanceMode 的 logical-type 感知转换
                rec.put(fieldName, convertToAvroType(entry.getValue(), tapField));
            } else {
                rec.put(fieldName, entry.getValue());
            }
        }
        return rec;
    }

    private GenericRecord buildHeadersRecord(Schema headersSchema, DMLType op, TapEvent tapEvent) {
        GenericRecord rec = new GenericData.Record(headersSchema);
        rec.put("operation", op.name());
        long ts = tapEvent.getTime() == null ? System.currentTimeMillis() : tapEvent.getTime();
        rec.put("timestamp", Instant.ofEpochMilli(ts).toString());
        return rec;
    }

    /**
     * 复制一份字段列表完全相同、但名字不同的 record schema。
     * 直接修改 inner 的 name 不被 Avro 允许，所以重新构造。
     */
    private Schema renameRecord(Schema source, String newName, String namespace) {
        Schema renamed = Schema.createRecord(newName, source.getDoc(), namespace, false);
        List<Schema.Field> copied = new ArrayList<>(source.getFields().size());
        for (Schema.Field f : source.getFields()) {
            Schema.Field nf = new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal(), f.order());
            for (String alias : f.aliases()) {
                nf.addAlias(alias);
            }
            copied.add(nf);
        }
        renamed.setFields(copied);
        return renamed;
    }

    /**
     * Attunity 风格 headers 子记录。除 operation 外其余字段在源端可能缺失，统一可空。
     */
    private Schema buildHeadersSchema(String name, String namespace) {
        Schema headers = Schema.createRecord(name, null, namespace, false);
        List<Schema.Field> fields = new ArrayList<>();
        fields.add(new Schema.Field("operation", Schema.create(Schema.Type.STRING), null, (Object) null));
        fields.add(nullableField("changeSequence", Schema.create(Schema.Type.STRING)));
        fields.add(nullableField("timestamp", Schema.create(Schema.Type.STRING)));
        fields.add(nullableField("streamPosition", Schema.create(Schema.Type.STRING)));
        fields.add(nullableField("transactionId", Schema.create(Schema.Type.STRING)));
        fields.add(nullableField("changeMask", Schema.create(Schema.Type.BYTES)));
        fields.add(nullableField("columnMask", Schema.create(Schema.Type.BYTES)));
        fields.add(nullableField("transactionEventCounter", Schema.create(Schema.Type.LONG)));
        fields.add(nullableField("transactionLastEvent", Schema.create(Schema.Type.BOOLEAN)));
        headers.setFields(fields);
        return headers;
    }

    private Schema.Field nullableField(String name, Schema type) {
        Schema nullable = Schema.createUnion(Schema.create(Schema.Type.NULL), type);
        return new Schema.Field(name, nullable, null, Schema.Field.NULL_DEFAULT_VALUE);
    }

}
