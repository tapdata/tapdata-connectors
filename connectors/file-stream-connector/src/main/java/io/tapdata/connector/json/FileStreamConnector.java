package io.tapdata.connector.json;

import io.tapdata.common.FileConfig;
import io.tapdata.common.FileConnector;
import io.tapdata.common.FileOffset;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapRawValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@TapConnectorClass("spec_filestream.json")
public class FileStreamConnector extends FileConnector {

    @Override
    protected void initConnection(TapConnectionContext connectorContext) throws Exception {
        fileConfig = new FileConfig().load(connectorContext.getConnectionConfig());
        super.initConnection(connectorContext);
    }

    @Override
    protected void readOneFile(FileOffset fileOffset, TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer, AtomicReference<List<TapEvent>> tapEvents) throws Exception {
        String path = fileOffset.getPath();
        long lastModified = storage.getFile(path).getLastModified();
        long size = storage.getFile(path).getLength();
        Map<String, Object> after = new HashMap<>();
        after.put("file_name", path.substring(path.lastIndexOf("/") + 1));
        after.put("file_path", path);
        after.put("file_size", size);
        after.put("last_modified", lastModified);
        after.put("file_data", storage.readFile(fileOffset.getPath()));
        TapRecordEvent tapEvent = insertRecordEvent(after, "file").referenceTime(lastModified);
        eventsOffsetConsumer.accept(Collections.singletonList(tapEvent), fileOffset);
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        codecRegistry.registerFromTapValue(TapRawValue.class, "STRING", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return tapRawValue.getValue().toString();
            return "null";
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss"));
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> formatTapDateTime(tapDateTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSSSSS"));
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd"));

        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
        connectorFunctions.supportWriteRecord(this::writeRecord);
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        TapTable tapTable = new TapTable("file");
        tapTable.add(new TapField("file_name", "STRING"));
        tapTable.add(new TapField("file_path", "STRING"));
        tapTable.add(new TapField("file_size", "NUMBER"));
        tapTable.add(new TapField("last_modified", "NUMBER"));
        tapTable.add(new TapField("file_data", "FILE"));
        consumer.accept(Collections.singletonList(tapTable));
    }

    protected long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Exception {
        return getFilteredFiles().size();
    }

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        AtomicLong insert = new AtomicLong(0);
        for (TapRecordEvent v : tapRecordEvents) {
            if (!(v instanceof TapInsertRecordEvent)) {
                continue;
            }
            Map<String, Object> map = ((TapInsertRecordEvent) v).getAfter();
            if (EmptyKit.isNotNull(map) && EmptyKit.isNotNull(map.get("file_data"))) {
                try (InputStream is = ((InputStream) map.get("file_data"))) {
                    storage.saveFile(correctPath(fileConfig.getFilePathString()) + map.get("file_name"), is, true);
                    insert.incrementAndGet();
                }
            }
        }
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
        writeListResultConsumer.accept(listResult.insertedCount(insert.get()));
    }

}
