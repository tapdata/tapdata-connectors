package io.tapdata.mongodb.reader;

import io.tapdata.entity.event.TapBaseEvent;
import org.bson.Document;

import java.util.Set;

import static io.tapdata.base.ConnectorBase.insertRecordEvent;

public interface StreamWithOpLogCollection {
    String OP_LOG_DB = "local";
    String OP_LOG_FULL_NAME ="local.oplog.rs";
    String OP_LOG_COLLECTION ="oplog.rs";

    default TapBaseEvent toOpLog(Document opLogEvent, Set<String> namespaces) {
        if (namespaces.contains(OP_LOG_FULL_NAME)) {
            return insertRecordEvent(opLogEvent, OP_LOG_COLLECTION);
        }
        return null;
    }
}
