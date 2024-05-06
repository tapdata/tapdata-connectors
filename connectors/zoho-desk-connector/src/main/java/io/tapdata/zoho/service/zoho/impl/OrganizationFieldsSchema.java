package io.tapdata.zoho.service.zoho.impl;

import cn.hutool.core.collection.CollUtil;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ZoHoOffset;
import io.tapdata.zoho.enums.ModuleEnums;
import io.tapdata.zoho.service.connectionMode.ConnectionMode;
import io.tapdata.zoho.service.zoho.loader.OrganizationFieldsOpenApi;
import io.tapdata.zoho.service.zoho.schema.Schemas;
import io.tapdata.zoho.utils.Checker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public class OrganizationFieldsSchema extends Schema implements SchemaLoader {
    private OrganizationFieldsOpenApi fieldLoader;

    @Override
    public String tableName() {
        return Schemas.OrganizationFields.getTableName();
    }

    @Override
    public SchemaLoader configSchema(TapConnectionContext tapConnectionContext) {
        this.fieldLoader = OrganizationFieldsOpenApi.create(tapConnectionContext);
        return this;
    }

    @Override
    public void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) {
        List<TapEvent> events = new ArrayList<>();
        TapConnectionContext context = fieldLoader.getContext();
        String modeName = context.getConnectionConfig().getString(CONNECTION_MODE);
        ConnectionMode connectionMode = ConnectionMode.getInstanceByName(context, modeName);
        ZoHoOffset zoHoOffset = initOffset(offset);
        String table = Schemas.OrganizationFields.getTableName();
        List<Map<String, Object>> listDepartment = fieldLoader.list(ModuleEnums.TICKETS, null, null);//分页数
        if (Checker.isEmpty(listDepartment) || listDepartment.isEmpty()) return;
        for (Map<String, Object> stringObjectMap : listDepartment) {
            if (!isAlive()) return;
            Map<String, Object> department = connectionMode.attributeAssignment(stringObjectMap, table, fieldLoader);
            if (CollUtil.isEmpty(department)) continue;
            zoHoOffset.getTableUpdateTimeMap().put(table, System.currentTimeMillis());
            events.add(TapSimplify.insertRecordEvent(department, table));
            if (events.size() == batchCount) {
                accept(consumer, events, zoHoOffset);
                events = new ArrayList<>();
            }
        }
        if (events.isEmpty()) return;
        accept(consumer, events, zoHoOffset);
    }

    @Override
    public long batchCount() {
        return this.fieldLoader.count(ModuleEnums.TASKS);
    }
}
