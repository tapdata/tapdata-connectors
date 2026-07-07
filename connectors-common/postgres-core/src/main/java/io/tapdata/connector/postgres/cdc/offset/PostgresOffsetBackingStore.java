package io.tapdata.connector.postgres.cdc.offset;

import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.kit.EmptyKit;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PostgresOffsetBackingStore extends MemoryOffsetBackingStore {

    private WorkerConfig config;

    public PostgresOffsetBackingStore() {
    }

    public void configure(WorkerConfig config) {
        super.configure(config);
        this.config = config;
        String slotName = (String) config.originals().get("slot.name");
    }

    public synchronized void start() {
        super.start();
        this.load();
    }

    public synchronized void stop() {
        super.stop();
    }

    private void load() {
        PostgresOffset postgresOffset = PostgresOffsetStorage.DBZ_OFFSET.get((String) config.originals().get("slot.name"));
        if (EmptyKit.isNull(postgresOffset) || EmptyKit.isNull(postgresOffset.getSourceOffset())) {
            this.data = new HashMap<>();
        } else {
            this.data.put(ByteBuffer.wrap(getOffsetKey().getBytes()),
                    ByteBuffer.wrap(postgresOffset.getSourceOffset().getBytes()));
        }
    }

    private String getOffsetKey() {
        Map<String, Object> map = new HashMap<>();
        map.put("schema", null);
        List<Object> list = TapSimplify.list();
        list.add(config.originals().get("name"));
        Map<String, Object> map1 = new HashMap<>();
        map1.put("server", config.originals().get("database.dbname"));
        list.add(map1);
        map.put("payload", list);
        return TapSimplify.toJson(map, JsonParser.ToJsonFeature.WriteMapNullValue);
    }

    protected void save() {
    }

}
