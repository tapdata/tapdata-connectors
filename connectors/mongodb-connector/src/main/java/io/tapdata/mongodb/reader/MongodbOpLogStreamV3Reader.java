package io.tapdata.mongodb.reader;

import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.mongodb.reader.v3.MongodbV3StreamReader;
import org.bson.Document;

import java.util.Optional;

public class MongodbOpLogStreamV3Reader extends MongodbV3StreamReader implements StreamWithOpLogCollection {

    @Override
    protected TapBaseEvent handleOplogEvent(Document event) {
        if (null == event) return null;
        TapBaseEvent tapBaseEvent = toOpLog(event, namespaces);
        return Optional.ofNullable(tapBaseEvent).orElse(super.handleOplogEvent(event));
    }
}
