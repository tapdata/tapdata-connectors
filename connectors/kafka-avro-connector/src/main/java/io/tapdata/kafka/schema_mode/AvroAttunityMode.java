package io.tapdata.kafka.schema_mode;

import io.tapdata.constant.DMLType;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kafka.IKafkaService;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static io.tapdata.constant.DMLType.*;

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
    // 消费侧 DDL 检测以 inner Data 子记录 schema 为基线，envelope 自身字段固定不变化
    private final Map<String, Schema> lastInnerSchemaPerTopic = new ConcurrentHashMap<>();

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

        Schema envelope = Schema.createRecord(kafkaService.getConfig().getNodeAttunityRecordName(), null, inner.getNamespace(), false);
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
        Schema opSchema = headersSchema.getField("operation").schema();
        rec.put("operation", new GenericData.EnumSymbol(opSchema, op.name()));
        if (tapEvent instanceof TapRecordEvent recordEvent) {
            rec.put("timestamp", recordEvent.getReferenceTime());
            rec.put("changeSequence", recordEvent.getExactlyOnceId());
            if (recordEvent.getInfo() != null) {
                rec.put("streamPosition", TapSimplify.toJson(recordEvent.getInfo().get("streamOffset")));
            }
        }

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
        Schema operationEnum = Schema.createEnum("operation", null, namespace,
                Arrays.asList(INSERT.name(), UPDATE.name(), DELETE.name()));
        List<Schema.Field> fields = new ArrayList<>();
        fields.add(new Schema.Field("operation", operationEnum, null, null));
        fields.add(nullableField("changeSequence", Schema.create(Schema.Type.STRING)));
        fields.add(nullableField("timestamp", LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))));
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

    // ---------------- 消费侧：拆 envelope，DDL 检测落在 inner Data 子记录 ----------------

    /**
     * 父类 {@link RegistryAvroMode#toTapEvents} 直接拿 GenericRecord 的 schema 做 DDL diff，
     * 对 envelope 来说 schema 永远是固定的 [Data, BeforeData, headers]，不会变化，DDL 检测会漏掉。
     * 这里改成对 inner {@code Data} 子记录的 schema 做 diff。
     */
    @Override
    public List<TapEvent> toTapEvents(ConsumerRecord<?, ?> consumerRecord) {
        if (consumerRecord == null || !(consumerRecord.value() instanceof GenericRecord)) {
            return super.toTapEvents(consumerRecord);
        }
        GenericRecord envelope = (GenericRecord) consumerRecord.value();
        Schema innerSchema = innerDataSchema(envelope.getSchema());
        if (innerSchema == null) {
            // envelope 结构异常，回落到父类裸路径
            return super.toTapEvents(consumerRecord);
        }
        String topic = consumerRecord.topic();
        List<TapEvent> events;
        Schema lastInner = lastInnerSchemaPerTopic.get(topic);
        if (lastInner == innerSchema) {
            TapEvent dml = toTapEvent(consumerRecord);
            return dml == null ? Collections.emptyList() : Collections.singletonList(dml);
        }
        if (lastInner == null) {
            TapTable knownTable = kafkaService.getConfig().tableMapGet(topic);
            events = knownTable != null
                    ? detectSchemaChangesFromTable(topic, knownTable, innerSchema, consumerRecord.timestamp())
                    : new ArrayList<>(1);
        } else {
            events = detectSchemaChanges(topic, lastInner, innerSchema, consumerRecord.timestamp());
        }
        lastInnerSchemaPerTopic.put(topic, innerSchema);
        TapEvent dml = toTapEvent(consumerRecord);
        if (dml != null) {
            events.add(dml);
        }
        return filterPrimaryKeyDDL(topic, events);
    }

    /**
     * 拆 envelope：
     * <ul>
     *     <li>op 优先取 {@code headers.operation} enum，回落到父类 Kafka header "op"</li>
     *     <li>INSERT/UPDATE: after = Data；UPDATE 同时 before = BeforeData</li>
     *     <li>DELETE: before = BeforeData</li>
     * </ul>
     * inner 子记录交给 {@link AvroEnhanceMode#convertGericRecordToMap}，logical type 自动还原。
     */
    @Override
    public TapEvent toTapEvent(ConsumerRecord<?, ?> consumerRecord) {
        if (consumerRecord == null || !(consumerRecord.value() instanceof GenericRecord)) {
            return null;
        }
        GenericRecord envelope = (GenericRecord) consumerRecord.value();
        DMLType op = readOperationFromEnvelope(envelope);
        if (op == null) {
            op = getOperationType(consumerRecord);
        }
        GenericRecord data = nestedRecord(envelope, kafkaService.getConfig().getNodeAttunityDataName());
        GenericRecord beforeData = nestedRecord(envelope, kafkaService.getConfig().getNodeAttunityBeforeDataName());

        TapRecordEvent event;
        switch (op) {
            case DELETE:
                TapDeleteRecordEvent del = TapDeleteRecordEvent.create();
                if (beforeData != null) del.setBefore(convertGericRecordToMap(beforeData));
                else if (data != null) del.setBefore(convertGericRecordToMap(data));
                event = del;
                break;
            case UPDATE:
                TapUpdateRecordEvent upd = TapUpdateRecordEvent.create();
                if (data != null) upd.setAfter(convertGericRecordToMap(data));
                if (beforeData != null) upd.setBefore(convertGericRecordToMap(beforeData));
                event = upd;
                break;
            case INSERT:
            default:
                TapInsertRecordEvent ins = TapInsertRecordEvent.create();
                if (data != null) ins.setAfter(convertGericRecordToMap(data));
                event = ins;
                break;
        }
        event.setTableId(consumerRecord.topic());
        event.setReferenceTime(consumerRecord.timestamp());
        return event;
    }

    /**
     * 从 envelope schema 中取出 Data 字段引用的 inner record schema。
     * Data 字段 schema 形如 ["null", innerRecord]，从 union 里挑非 NULL 分支。
     */
    private Schema innerDataSchema(Schema envelopeSchema) {
        if (envelopeSchema == null || envelopeSchema.getType() != Schema.Type.RECORD) {
            return null;
        }
        Schema.Field dataField = envelopeSchema.getField(kafkaService.getConfig().getNodeAttunityDataName());
        if (dataField == null) return null;
        Schema fs = dataField.schema();
        if (fs.getType() == Schema.Type.UNION) {
            for (Schema t : fs.getTypes()) {
                if (t.getType() == Schema.Type.RECORD) return t;
            }
            return null;
        }
        return fs.getType() == Schema.Type.RECORD ? fs : null;
    }

    private GenericRecord nestedRecord(GenericRecord envelope, String fieldName) {
        if (envelope.getSchema().getField(fieldName) == null) return null;
        Object v = envelope.get(fieldName);
        return v instanceof GenericRecord ? (GenericRecord) v : null;
    }

    /**
     * 读 headers.operation enum。Attunity envelope 把 op 写在子记录里，
     * 与父类按 Kafka header 取 op 的约定不同，这里优先走 envelope 自身的语义。
     */
    private DMLType readOperationFromEnvelope(GenericRecord envelope) {
        GenericRecord headers = nestedRecord(envelope, kafkaService.getConfig().getNodeAttunityHeadersName());
        if (headers == null) return null;
        if (headers.getSchema().getField("operation") == null) return null;
        Object opVal = headers.get("operation");
        if (opVal == null) return null;
        try {
            return DMLType.valueOf(opVal.toString().toUpperCase());
        } catch (IllegalArgumentException ignore) {
            return null;
        }
    }

}
