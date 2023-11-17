package io.tapdata.connector.redis.writer;

import com.moilioncircle.redis.replicator.cmd.impl.DefaultCommand;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpKeyValuePair;
import com.moilioncircle.redis.replicator.util.Strings;
import io.tapdata.connector.redis.RedisContext;
import io.tapdata.connector.redis.RedisPipeline;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import redis.clients.jedis.Protocol;

public class RedisCopyRecordWriter extends AbstractRedisRecordWriter {

    private int dbNumber = 0;

    public RedisCopyRecordWriter(RedisContext redisContext, TapTable tapTable) {
        super(redisContext, tapTable);
    }

    @Override
    protected void handleInsertEvent(TapInsertRecordEvent event, RedisPipeline pipelined) {
        Object redisEvent = event.getInfo().get("redis_event");
        if (redisEvent instanceof DumpKeyValuePair) {
            DumpKeyValuePair dkv = (DumpKeyValuePair) redisEvent;
            DB db = dkv.getDb();
            int index;
            if (db != null && (index = (int) db.getDbNumber()) != this.dbNumber) {
                pipelined.select(index);
                dbNumber = index;
            }
            if (EmptyKit.isNull(dkv.getExpiredMs())) {
                pipelined.set(new String(dkv.getKey()), new String(dkv.getValue()));
            } else {
                long ms = dkv.getExpiredMs() - System.currentTimeMillis();
                if (ms <= 0) {
                    return;
                }
                pipelined.setex(new String(dkv.getKey()), ms / 1000, new String(dkv.getValue()));
            }
        } else if (redisEvent instanceof DefaultCommand) {
            DefaultCommand commandEvent = (DefaultCommand) redisEvent;
            pipelined.sendCommand(Protocol.Command.valueOf(Strings.toString(commandEvent.getCommand()).toUpperCase()), commandEvent.getArgs());
        }
    }

    @Override
    protected void handleUpdateEvent(TapUpdateRecordEvent event, RedisPipeline pipelined) throws Exception {

    }

    @Override
    protected void handleDeleteEvent(TapDeleteRecordEvent event, RedisPipeline pipelined) {

    }
}
