package io.tapdata.connector.starrocks.streamload;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.connector.starrocks.StarrocksJdbcContext;
import io.tapdata.connector.starrocks.bean.StarrocksConfig;
import io.tapdata.connector.starrocks.streamload.exception.StarrocksRetryableException;
import io.tapdata.connector.starrocks.streamload.exception.StarrocksRuntimeException;
import io.tapdata.connector.starrocks.streamload.exception.StreamLoadException;
import io.tapdata.connector.starrocks.streamload.rest.models.RespContent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.writeListResult;

/**
 * @author jarad
 * @date 7/14/22
 */
public class StarrocksStreamLoader {
    private static final String TAG = StarrocksStreamLoader.class.getSimpleName();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String HTTPS_LOAD_URL_PATTERN = "https://%s/api/%s/%s/_stream_load";
    private static final String HTTP_LOAD_URL_PATTERN = "http://%s/api/%s/%s/_stream_load";
    private static final String LABEL_PREFIX_PATTERN = "tapdata_%s_%s";

    private final StarrocksConfig StarrocksConfig;
    private final CloseableHttpClient httpClient;
    private final RecordStream recordStream;

    private boolean loadBatchFirstRecord;
    private AtomicInteger lastEventFlag;
    private final AtomicReference<Set<String>> dataColumns;
    private MessageSerializer messageSerializer;
    private TapTable tapTable;
    private final Metrics metrics;

    public StarrocksStreamLoader(StarrocksJdbcContext StarrocksJdbcContext, CloseableHttpClient httpClient) {
        this.StarrocksConfig = (StarrocksConfig) StarrocksJdbcContext.getConfig();
        this.httpClient = httpClient;
        Integer writeByteBufferCapacity = StarrocksConfig.getWriteByteBufferCapacity();
        if (null == writeByteBufferCapacity) {
            writeByteBufferCapacity = Constants.CACHE_BUFFER_SIZE;
        } else {
            writeByteBufferCapacity = writeByteBufferCapacity * 1024;
        }
        this.recordStream = new RecordStream(writeByteBufferCapacity, Constants.CACHE_BUFFER_COUNT);
        this.loadBatchFirstRecord = true;
        this.lastEventFlag = new AtomicInteger(0);
        this.dataColumns = new AtomicReference<>();
        initMessageSerializer();
        this.metrics = new Metrics();
    }

    private void initMessageSerializer() {
        StarrocksConfig.WriteFormat writeFormat = StarrocksConfig.getWriteFormatEnum();
        TapLogger.info(TAG, "Starrocks stream load run with {} format", writeFormat);
        switch (writeFormat) {
            case csv:
                messageSerializer = new CsvSerializer();
                break;
            case json:
                messageSerializer = new JsonSerializer();
                break;
        }
    }

    public void writeRecord(final List<TapRecordEvent> tapRecordEvents, final TapTable table, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        try {
            TapLogger.debug(TAG, "Batch events length is: {}", tapRecordEvents.size());
            WriteListResult<TapRecordEvent> listResult = writeListResult();
            this.tapTable = table;
            boolean isAgg = StarrocksTableType.Aggregate.toString().equals(StarrocksConfig.getUniqueKeyType());
            for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
                byte[] bytes = messageSerializer.serialize(table, tapRecordEvent, isAgg);
                if (needFlush(tapRecordEvent, bytes.length, isAgg)) {
                    flush(table, listResult);
                }
                if (lastEventFlag.get() == 0) {
                    startLoad(tapRecordEvent);
                }
                writeRecord(bytes);
                metrics.increase(tapRecordEvent);
            }
            flush(table, listResult);
            writeListResultConsumer.accept(listResult);
        } catch (Throwable e) {
            recordStream.init();
            throw e;
        }
    }

    public void writeRecord(byte[] record) throws IOException {
        if (loadBatchFirstRecord) {
            loadBatchFirstRecord = false;
        } else {
            recordStream.write(messageSerializer.lineEnd());
        }
        recordStream.write(record);
    }

    public void startLoad(final TapRecordEvent recordEvent) throws IOException {
        recordStream.startInput();
        recordStream.write(messageSerializer.batchStart());
        lastEventFlag.set(OperationType.getOperationFlag(recordEvent));
        dataColumns.set(getDataColumns(recordEvent));
        loadBatchFirstRecord = true;
    }

    private Set<String> getDataColumns(TapRecordEvent recordEvent) {
        if (recordEvent instanceof TapInsertRecordEvent) {
            return ((TapInsertRecordEvent) recordEvent).getAfter().keySet();
        } else if (recordEvent instanceof TapUpdateRecordEvent) {
            return ((TapUpdateRecordEvent) recordEvent).getAfter().keySet();
        } else if (recordEvent instanceof TapDeleteRecordEvent) {
            return ((TapDeleteRecordEvent) recordEvent).getBefore().keySet();
        }
        return Collections.emptySet();
    }

    public RespContent put(final TapTable table) throws StreamLoadException, StarrocksRetryableException {
        StarrocksConfig.WriteFormat writeFormat = StarrocksConfig.getWriteFormatEnum();
        try {
            final String loadUrl = buildLoadUrl(StarrocksConfig.getStarrocksHttp(), StarrocksConfig.getDatabase(), table.getId());
            final String prefix = buildPrefix(table.getId());

            String label = prefix + "-" + UUID.randomUUID();
            List<String> columns = new ArrayList<>();
            for (String col : tapTable.getNameFieldMap().keySet()) {
                if (dataColumns.get().contains(col) || StarrocksTableType.Aggregate.toString().equals(StarrocksConfig.getUniqueKeyType())) {
                    columns.add("`" + col + "`");
                }
            }
            // add the Starrocks_DELETE_SIGN at the end of the column
            columns.add(Constants.Starrocks_DELETE_SIGN);
            HttpPutBuilder putBuilder = new HttpPutBuilder();
            InputStreamEntity entity = new InputStreamEntity(recordStream, recordStream.getContentLength());
            entity.setContentType("application/json");
            putBuilder.setUrl(loadUrl)
                    // 前端表单传出来的值和tdd json加载的值可能有差别，如前端传的pwd可能是null，tdd的是空字符串
                    .baseAuth(StarrocksConfig.getUser(), StarrocksConfig.getPassword())
                    .addCommonHeader()
                    .addFormat(writeFormat)
                    .addColumns(columns)
                    .setLabel(label)
                    .setEntity(entity);
            Collection<String> primaryKeys = table.primaryKeys(true);
            if (CollectionUtils.isEmpty(primaryKeys)) {
                putBuilder.enableAppend();
            } else {
                if (StarrocksTableType.Unique.toString().equals(StarrocksConfig.getUniqueKeyType())) {
                    putBuilder.enableDelete();
                    putBuilder.addPartialHeader();
                } else {
                    putBuilder.enableAppend();
                }
            }
            HttpPut httpPut = putBuilder.build();
            TapLogger.debug(TAG, "Call stream load http api, url: {}, headers: {}", loadUrl, putBuilder.header);
            try (CloseableHttpResponse execute = httpClient.execute(httpPut)) {
                return handlePreCommitResponse(execute);
            }
        } catch (StarrocksRetryableException e) {
            throw e;
        } catch (Exception e) {
            throw new StreamLoadException(String.format("Call stream load error: %s", e.getMessage()), e);
        }
    }

    public RespContent handlePreCommitResponse(CloseableHttpResponse response) throws Exception {
        final int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != 200 || response.getEntity() == null) {
            throw new StarrocksRetryableException("Stream load error: " + response.getStatusLine().toString());
        }
        String loadResult = EntityUtils.toString(response.getEntity());

        TapLogger.debug(TAG, "Stream load Result {}", loadResult);
        RespContent respContent = OBJECT_MAPPER.readValue(loadResult, RespContent.class);
        if (!respContent.isSuccess() && !"Publish Timeout".equals(respContent.getStatus())) {
            if (respContent.getMessage().toLowerCase().contains("too many filtered rows")
                    || respContent.getMessage().toLowerCase().contains("access denied")) {
                throw new StreamLoadException("Stream load failed | Error: " + loadResult);
            }
            throw new StarrocksRetryableException(loadResult);
        }
        return respContent;
    }

    public RespContent flush(TapTable table) throws StarrocksRetryableException {
        return flush(table, null);
    }

    public RespContent flush(TapTable table, WriteListResult<TapRecordEvent> listResult) throws StarrocksRetryableException {
        // the stream is not started yet, no response to get
        if (lastEventFlag.get() == 0) {
            return null;
        }
        try {
            recordStream.write(messageSerializer.batchEnd());
            recordStream.endInput();
            RespContent respContent = put(table);
            TapLogger.info(TAG, "Execute stream load response: " + respContent);
            if (null != listResult) {
                metrics.writeIntoResultList(listResult);
                metrics.clear();
            }
            return respContent;
        } catch (StarrocksRetryableException e) {
            throw e;
        } catch (Exception e) {
            throw new StarrocksRuntimeException(e);
        } finally {
            lastEventFlag.set(0);
            recordStream.setContentLength(0L);
        }
    }

    public void shutdown() {
        try {
//            this.stopLoad();
            this.httpClient.close();
        } catch (Exception ignored) {
        }
    }

    protected String buildLoadUrl(final String StarrocksHttp, final String database, final String tableName) {
        if (Boolean.TRUE.equals(StarrocksConfig.getUseHTTPS()))
            return String.format(HTTPS_LOAD_URL_PATTERN, StarrocksHttp, database, tableName);
        else
            return String.format(HTTP_LOAD_URL_PATTERN, StarrocksHttp, database, tableName);
    }

    private String buildPrefix(final String tableName) {
        return String.format(LABEL_PREFIX_PATTERN, Thread.currentThread().getId(), tableName);
    }

    protected boolean needFlush(TapRecordEvent recordEvent, int length, boolean noNeed) {
        int lastEventType = lastEventFlag.get();
        return lastEventType > 0 && !getDataColumns(recordEvent).equals(dataColumns.get()) && !noNeed
                || !recordStream.canWrite(length);
    }

    private void stopLoad() throws StarrocksRetryableException {
        if (null != tapTable) {
            flush(tapTable);
        }
    }

    public enum OperationType {
        INSERT(1, "insert"),
        UPDATE(2, "update"),
        DELETE(3, "delete");

        private final int code;
        private final String desc;

        OperationType(int code, String desc) {
            this.code = code;
            this.desc = desc;
        }

        static int getOperationFlag(TapRecordEvent recordEvent) {
            if (recordEvent instanceof TapInsertRecordEvent) {
                return INSERT.code;
            } else if (recordEvent instanceof TapUpdateRecordEvent) {
                return UPDATE.code;
            } else if (recordEvent instanceof TapDeleteRecordEvent) {
                return DELETE.code;
            } else {
                throw new UnsupportedOperationException();
            }
        }
    }

    private static class Metrics {
        private long insert = 0L;
        private long update = 0L;
        private long delete = 0L;

        public Metrics() {
        }

        public void increase(TapRecordEvent tapRecordEvent) {
            if (tapRecordEvent instanceof TapInsertRecordEvent) {
                insert++;
            } else if (tapRecordEvent instanceof TapUpdateRecordEvent) {
                update++;
            } else if (tapRecordEvent instanceof TapDeleteRecordEvent) {
                delete++;
            }
        }

        public void clear() {
            insert = 0L;
            update = 0L;
            delete = 0L;
        }

        public void writeIntoResultList(WriteListResult<TapRecordEvent> listResult) {
            listResult.incrementInserted(insert);
            listResult.incrementModified(update);
            listResult.incrementRemove(delete);
        }
    }
}
