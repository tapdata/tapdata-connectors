package io.tapdata.connector.elasticsearch;

import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.toJson;

public class ElasticsearchRecordWriter {

    private Log log;
    private String insertPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
    private String updatePolicy = ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS;
    private String version;
    private RestHighLevelClient client;
    private final TapTable tapTable;
    private List<String> uniqueCondition;
    private boolean hasPk = false;
    private WriteListResult<TapRecordEvent> listResult;
    private final MessageDigest md = MessageDigest.getInstance("SHA-256");

    private ElasticsearchExceptionCollector exceptionCollector;

    public ElasticsearchRecordWriter(ElasticsearchHttpContext httpContext, TapTable tapTable, String insertPolicy, String updatePolicy) throws Throwable {
        this.client = httpContext.getElasticsearchClient();
        this.tapTable = tapTable;
        this.exceptionCollector = new ElasticsearchExceptionCollector();
        analyzeTable();
        if (StringUtils.isNotBlank(insertPolicy)) {
            this.insertPolicy = insertPolicy;
        }
        if (StringUtils.isNotBlank(updatePolicy)) {
            this.updatePolicy = updatePolicy;
        }
    }

    private void analyzeTable() {
        //1、primaryKeys has first priority
        if (EmptyKit.isNotEmpty(tapTable.primaryKeys(false))) {
            hasPk = true;
            uniqueCondition = new ArrayList<>(tapTable.primaryKeys(false));
        }
        //2、second priority: analyze table with its indexes
        else {
            uniqueCondition = new ArrayList<>(tapTable.primaryKeys(true));
        }
    }

    public void write(List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        //result of these events
        listResult = new WriteListResult<>();
        try{
            BulkRequest bulkRequest = new BulkRequest();
//        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (TapRecordEvent recordEvent : tapRecordEvents) {
                if (recordEvent instanceof TapInsertRecordEvent) {
                    if (ConnectionOptions.DML_INSERT_POLICY_JUST_INSERT.equals(insertPolicy)) {
                        bulkRequest.add(insertDocument(recordEvent));
                    } else if (ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS.equals(insertPolicy)) {
                        bulkRequest.add(upsertDocument(recordEvent));
                    } else if (ConnectionOptions.DML_INSERT_POLICY_IGNORE_ON_EXISTS.equals(insertPolicy)) {
                         throw new RuntimeException("Unsupported insert policy: " + insertPolicy);
                    }
                } else if (recordEvent instanceof TapUpdateRecordEvent) {
                    if (ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS.equals(updatePolicy)) {
                        bulkRequest.add(updateDocument(recordEvent));
                    } else if (ConnectionOptions.DML_UPDATE_POLICY_INSERT_ON_NON_EXISTS.equals(updatePolicy)) {
                        bulkRequest.add(upsertDocument(recordEvent));
                    }
                } else if (recordEvent instanceof TapDeleteRecordEvent) {
                    bulkRequest.add(deleteDocument(recordEvent));
                }
            }
            BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            long insertCount = 0L;
            long updateCount = 0L;
            long deleteCount = 0L;
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                if (bulkItemResponse.isFailed()) {
                    BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                    throw failure.getCause();
                }
                switch (bulkItemResponse.getOpType()) {
                    case INDEX:
                    case CREATE:
                            insertCount++;
                        break;
                    case UPDATE:
                            updateCount++;
                        break;
                    case DELETE:
                            deleteCount++;
                        break;
                    default:
                        break;
                }
            }
            writeListResultConsumer.accept(listResult
                    .insertedCount(insertCount)
                    .modifiedCount(updateCount)
                    .removedCount(deleteCount));
        }catch (Throwable t){
            log.warn("{}: {}", t.getMessage(), toJson(t.getStackTrace()));
            TapRecordEvent errorEvent = null;
            if(listResult.getErrorMap() != null){
                 errorEvent = listResult.getErrorMap().keySet().stream().findFirst().orElse(null);
            }
            exceptionCollector.collectTerminateByServer(t);
            exceptionCollector.collectWritePrivileges("writeRecord", Collections.emptyList(), t);
            if(errorEvent != null){
                exceptionCollector.collectWriteType(null, null, errorEvent, t);
                exceptionCollector.collectWriteLength(null,null,errorEvent,t);
            }
            throw new RuntimeException(t);
        }

    }

    public ElasticsearchRecordWriter setVersion(String version) {
        this.version = version;
        return this;
    }

    public ElasticsearchRecordWriter setInsertPolicy(String insertPolicy) {
        this.insertPolicy = insertPolicy;
        return this;
    }

    public ElasticsearchRecordWriter setUpdatePolicy(String updatePolicy) {
        this.updatePolicy = updatePolicy;
        return this;
    }

    private IndexRequest insertDocument(TapRecordEvent recordEvent) {
        TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
        Map<String, Object> value = insertRecordEvent.getAfter();
        String id = getId(value).toString();
        return new IndexRequest(tapTable.getId().toLowerCase()).id(id).source(value);
    }

    private UpdateRequest updateDocument(TapRecordEvent recordEvent) {
        TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
        Map<String, Object> value = updateRecordEvent.getAfter();
        String id = getId(value).toString();
        return new UpdateRequest(tapTable.getId().toLowerCase(), id).retryOnConflict(3).doc(value);
    }

    private UpdateRequest upsertDocument(TapRecordEvent recordEvent) {
        Map<String, Object> value;
        String id;
        if (recordEvent instanceof TapInsertRecordEvent) {
            TapInsertRecordEvent tapInsertRecordEvent = (TapInsertRecordEvent) recordEvent;
            value = tapInsertRecordEvent.getAfter();
            id = getId(value).toString();
        } else if (recordEvent instanceof TapUpdateRecordEvent) {
            TapUpdateRecordEvent tapUpdateRecordEvent = (TapUpdateRecordEvent) recordEvent;
            value = tapUpdateRecordEvent.getAfter();
            Map<String, Object> before = tapUpdateRecordEvent.getBefore();
            if (null != before) {
                id = getId(before).toString();
                if (StringUtils.isBlank(id)) {
                    id = getId(value).toString();
                }
            } else {
                id = getId(value).toString();
            }
        } else {
            throw new RuntimeException("Unsupported record event type: " + recordEvent.getClass().getName());
        }
        return new UpdateRequest(tapTable.getId().toLowerCase(), id).retryOnConflict(3).doc(value).docAsUpsert(true);
    }

    private DeleteRequest deleteDocument(TapRecordEvent recordEvent) {
        TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
        Map<String, Object> value = deleteRecordEvent.getBefore();
        String id = getId(value).toString();
        return new DeleteRequest(tapTable.getId().toLowerCase(), id);
    }

    private StringBuilder getId(Map<String, Object> value) {
        StringBuilder ids = new StringBuilder();
        ids.append(tapTable.getId().toLowerCase()).append("_");
        if (EmptyKit.isNotEmpty(uniqueCondition)) {
            for (String condition : uniqueCondition) {
                ids.append(object2String(value.get(condition))).append("_");
            }
        }

        // ES will check and limit the size of id smaller than 512 bytes
        // hash id using "SHA-256" if teh size exceeds 512 bytes
        String idsStr = ids.toString();
        if (idsStr.getBytes(StandardCharsets.UTF_8).length > 512) {
            byte[] hash = md.digest(idsStr.getBytes(StandardCharsets.UTF_8));

            // convert hash into hex string
            BigInteger number = new BigInteger(1, hash);
            // Convert message digest into hex value
            StringBuilder hex = new StringBuilder(number.toString(16));
            while (hex.length() < 32) {
                hex.insert(0, '0');
            }
            ids = hex;
        }

        value.remove("_id");
        return ids;
    }

    private String object2String(Object object) {
        if (object == null) {
            return "null";
        }
        if (object instanceof byte[]) {
            return new String((byte[]) object);
        }
        return object.toString();
    }

    public ElasticsearchRecordWriter log(Log log) {
        this.log = log;
        return this;
    }
}
