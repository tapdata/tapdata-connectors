package io.tapdata.zoho.service.zoho.impl;

import cn.hutool.core.collection.CollUtil;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ZoHoOffset;
import io.tapdata.zoho.service.connectionMode.ConnectionMode;
import io.tapdata.zoho.service.zoho.loader.SkillsOpenApi;
import io.tapdata.zoho.service.zoho.schema.Schemas;

import java.util.*;
import java.util.function.BiConsumer;

public class SkillsSchema extends Schema implements SchemaLoader {
    SkillsOpenApi skillsOpenApi;

    @Override
    public String tableName() {
        return Schemas.Skills.getTableName();
    }

    @Override
    public SchemaLoader configSchema(TapConnectionContext tapConnectionContext) {
        this.skillsOpenApi = SkillsOpenApi.create(tapConnectionContext);
        return this;
    }

    @Override
    public void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) {
        this.read(batchCount, offset, consumer, Boolean.FALSE);
    }

    @Override
    public long batchCount() {
        return 0;
    }

    public void read(int readSize, Object offsetState, BiConsumer<List<TapEvent>, Object> consumer, boolean isStreamRead) {
        final List<TapEvent>[] events = new List[]{new ArrayList<>()};
        int pageSize = Math.min(readSize, SkillsOpenApi.MAX_PAGE_LIMIT);
        int fromPageIndex = 1;
        TapConnectionContext context = this.skillsOpenApi.getContext();
        String modeName = context.getConnectionConfig().getString(CONNECTION_MODE);
        ConnectionMode connectionMode = ConnectionMode.getInstanceByName(context, modeName);
        String tableName = Schemas.Skills.getTableName();
        String departmentId = "";
        final ZoHoOffset offset = initOffset(offsetState);
        while (isAlive()) {
            List<Map<String, Object>> list = skillsOpenApi.page(departmentId, fromPageIndex, pageSize);
            if (CollUtil.isEmpty(list)) break;
            fromPageIndex += pageSize;
            list.stream().filter(Objects::nonNull).forEach(product -> {
                if (!isAlive()) return;
                Map<String, Object> oneProduct = connectionMode.attributeAssignment(product, tableName, skillsOpenApi);
                if (CollUtil.isEmpty(oneProduct)) return;
                long referenceTime = referenceTime(offset, oneProduct, tableName, isStreamRead);
                acceptOne(offset, readSize, events, oneProduct, referenceTime, consumer, isStreamRead);
            });
        }
        if (!events[0].isEmpty()) {
            accept(consumer, events[0], offsetState);
        }
    }

    protected void acceptOne(ZoHoOffset offset, int readSize, List<TapEvent>[] events, Map<String, Object> oneProduct, long referenceTime, BiConsumer<List<TapEvent>, Object> consumer, boolean isStreamRead) {
        TapInsertRecordEvent tapInsertRecordEvent = TapSimplify.insertRecordEvent(oneProduct, tableName());
        if (isStreamRead) {
            tapInsertRecordEvent.referenceTime(referenceTime);
        }
        events[0].add(tapInsertRecordEvent);
        if (events[0].size() == readSize) {
            accept(consumer, events[0], offset);
            events[0] = new ArrayList<>();
        }
    }
}
