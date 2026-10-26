package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.PaimonConnector;
import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.impl.ToTapDateCodec;
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PaimonDateRoundTripTest {

    private static final String DATABASE = "default";
    private static final String TABLE = "date_round_trip";

    @TempDir
    Path tempDir;

    @ParameterizedTest(name = "persist {0} {1}")
    @CsvSource({"sql.Date,1980-01-01", "sql.Date,2026-10-15",
            "text,1980-01-01", "text,2026-10-15",
            "LocalDateTime,1980-01-01", "LocalDateTime,2026-10-15",
            "textWithTime,1980-01-01", "textWithTime,2026-10-15",
            "illegalZero,0000-00-00", "zeroText,0000-00-00"})
    void persistedDateMustKeepSourceCalendarDay(String kind, String text) throws Exception {
        TimeZone previous = TimeZone.getDefault();
        PaimonService service = null;
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
            service = new PaimonService(config(), mock(Log.class));
            service.init();
            Catalog catalog = catalog(service);
            catalog.createDatabase(DATABASE, true);
            catalog.createTable(Identifier.create(DATABASE, TABLE), Schema.newBuilder()
                    .column("id", DataTypes.INT()).column("d", DataTypes.DATE())
                    .primaryKey("id").option("bucket", "1").option("write-buffer-size", "8mb")
                    .build(), false);
            TapTable tapTable = new TapTable(TABLE)
                    .add(new TapField("id", "INT").primaryKeyPos(1))
                    .add(new TapField("d", "DATE"));
            boolean illegal = kind.equals("illegalZero");
            boolean zero = text.equals("0000-00-00");
            LocalDate expected = zero ? null : LocalDate.parse(text);
            Object origin = zero ? text : kind.equals("sql.Date") ? java.sql.Date.valueOf(text)
                    : kind.equals("LocalDateTime") ? expected.atStartOfDay()
                    : kind.equals("textWithTime") ? text + " 00:00:00" : text;
            TapDateValue value = illegal
                    ? new TapDateValue(new DateTime(text, DateTime.DATE_TYPE))
                    : new ToTapDateCodec().toTapValue(origin, new TapDate());
            value.setOriginValue(origin);
            TapCodecsRegistry registry = new TapCodecsRegistry();
            new PaimonConnector().registerCapabilities(new ConnectorFunctions(), registry);
            Object converted = registry.getFromTapValueCodec(TapDateValue.class).fromTapValue(value);
            service.writeRecords(Collections.singletonList(cdcInsert(map("id", 1, "d", converted))),
                    tapTable, context());
            List<Map<String, Object>> rows = castRows(service.executeQuery("SELECT * FROM " + TABLE, mock(Log.class)));
            assertEquals(1, rows.size());
            Object raw = rows.get(0).get("d");
            LocalDate stored = raw == null ? null : new ToTapDateCodec().toTapValue(raw, new TapDate())
                    .getValue().toLocalDateTime().toLocalDate();
            assertEquals(expected, stored);
        } finally {
            try {
                if (service != null) {
                    service.close();
                }
            } finally {
                TimeZone.setDefault(previous);
            }
        }
    }

    private PaimonConfig config() throws Exception {
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse(Files.createDirectories(tempDir.resolve("warehouse")).toString());
        config.setStorageType("local");
        config.setDatabase(DATABASE);
        config.setDiskTmpDir(Files.createDirectories(tempDir.resolve("spill")).toString());
        config.setBatchAccumulationSize(0);
        config.setCommitIntervalMs(0);
        config.setEnableAsyncCommit(false);
        config.setWriteBufferSize(8);
        return config;
    }

    private TapInsertRecordEvent cdcInsert(Map<String, Object> after) {
        TapInsertRecordEvent event = new TapInsertRecordEvent().init().table(TABLE).after(after);
        event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "CDC");
        event.addInfo("nodeIds", Collections.singletonList("test-source"));
        return event;
    }

    @SuppressWarnings("unchecked")
    private TapConnectorContext context() {
        Map<String, Object> values = new ConcurrentHashMap<>();
        KVMap<Object> stateMap = mock(KVMap.class);
        when(stateMap.get(anyString())).thenAnswer(invocation -> values.get(invocation.getArgument(0)));
        when(stateMap.putIfAbsent(anyString(), any())).thenAnswer(
                invocation -> values.putIfAbsent(invocation.getArgument(0), invocation.getArgument(1)));
        doAnswer(invocation -> {
            values.put(invocation.getArgument(0), invocation.getArgument(1));
            return null;
        }).when(stateMap).put(anyString(), any());

        TapConnectorContext context = mock(TapConnectorContext.class);
        when(context.getStateMap()).thenReturn(stateMap);
        when(context.getLog()).thenReturn(mock(Log.class));
        return context;
    }

    private Catalog catalog(PaimonService service) throws Exception {
        java.lang.reflect.Field field = PaimonService.class.getDeclaredField("catalog");
        field.setAccessible(true);
        return (Catalog) field.get(service);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> castRows(Object result) {
        return (List<Map<String, Object>>) result;
    }

    private Map<String, Object> map(Object... values) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            result.put((String) values[i], values[i + 1]);
        }
        return result;
    }
}
