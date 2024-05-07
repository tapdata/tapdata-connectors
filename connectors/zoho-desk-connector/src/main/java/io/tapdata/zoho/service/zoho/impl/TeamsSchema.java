package io.tapdata.zoho.service.zoho.impl;

import cn.hutool.core.collection.CollUtil;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ZoHoOffset;
import io.tapdata.zoho.service.connection.ConnectionMode;
import io.tapdata.zoho.service.zoho.loader.TeamsOpenApi;
import io.tapdata.zoho.service.zoho.schema.Schemas;
import io.tapdata.zoho.utils.Checker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public class TeamsSchema extends Schema implements SchemaLoader {
    TeamsOpenApi teamsOpenApi;

    @Override
    public String tableName() {
        return Schemas.Teams.getTableName();
    }

    @Override
    public SchemaLoader configSchema(TapConnectionContext tapConnectionContext) {
        this.teamsOpenApi = TeamsOpenApi.create(tapConnectionContext);
        return this;
    }

    @Override
    public void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) {
        this.read(batchCount, offset, consumer, Boolean.TRUE);
    }

    @Override
    public long batchCount() {
        return 0;
    }

    public void read(int readSize, Object offsetState, BiConsumer<List<TapEvent>, Object> consumer, boolean isBatchRead) {
        TapConnectionContext context = this.teamsOpenApi.getContext();
        String modeName = context.getConnectionConfig().getString(CONNECTION_MODE);
        ConnectionMode connectionMode = ConnectionMode.getInstanceByName(context, modeName);
        String tableName = Schemas.Teams.getTableName();
        List<Map<String, Object>> list = teamsOpenApi.page();
        if (Checker.isEmpty(list) || list.isEmpty()) return;
        List<TapEvent> events = new ArrayList<>();
        ZoHoOffset offset = initOffset(offsetState);
        for (Map<String, Object> product : list) {
            if (!isAlive()) return;
            Map<String, Object> oneProduct = connectionMode.attributeAssignment(product, tableName, teamsOpenApi);
            if (CollUtil.isEmpty(oneProduct)) continue;
            TapInsertRecordEvent tapInsertRecordEvent = TapSimplify.insertRecordEvent(oneProduct, tableName);
            if (!isBatchRead) {
                tapInsertRecordEvent.referenceTime(System.currentTimeMillis());
            }
            events.add(tapInsertRecordEvent);
            if (events.size() == readSize) {
                accept(consumer, events, offset);
                events = new ArrayList<>();
            }
        }
        if (!events.isEmpty()) {
            accept(consumer, events, offset);
        }
    }
}
