package io.tapdata.zoho.service.zoho.impl;

import cn.hutool.core.collection.CollUtil;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ZoHoOffset;
import io.tapdata.zoho.service.connection.ConnectionMode;
import io.tapdata.zoho.service.zoho.loader.DepartmentOpenApi;
import io.tapdata.zoho.service.zoho.schema.Schemas;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public class DepartmentsSchema extends Schema implements SchemaLoader {
    private DepartmentOpenApi departmentOpenApi;

    @Override
    public String tableName() {
        return Schemas.Departments.getTableName();
    }

    @Override
    public SchemaLoader configSchema(TapConnectionContext tapConnectionContext) {
        this.departmentOpenApi = DepartmentOpenApi.create(tapConnectionContext);
        return this;
    }

    @Override
    public void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) {
        final List<TapEvent>[] events = new List[]{new ArrayList<>()};
        int pageSize = Math.min(batchCount, DepartmentOpenApi.MAX_PAGE_LIMIT);
        // page start from 0
        int fromPageIndex = 0;
        TapConnectionContext context = departmentOpenApi.getContext();
        String modeName = context.getConnectionConfig().getString(CONNECTION_MODE);
        ConnectionMode connectionMode = ConnectionMode.getInstanceByName(context, modeName);
        final ZoHoOffset zoHoOffset = initOffset(offset);
        while (isAlive()) {
            List<Map<String, Object>> listDepartment = departmentOpenApi.list(null, null, fromPageIndex, pageSize, null);//分页数
            if (CollUtil.isEmpty(listDepartment)) {
                break;
            }
            fromPageIndex += pageSize;
            eachAll(listDepartment, connectionMode, zoHoOffset, events, consumer, batchCount);
        }
        if (!events[0].isEmpty()) {
            accept(consumer, events[0], zoHoOffset);
        }
    }

    private void eachAll(List<Map<String, Object>> listDepartment, ConnectionMode connectionMode, ZoHoOffset zoHoOffset, List<TapEvent>[] events, BiConsumer<List<TapEvent>, Object> consumer, int batchCount) {
        String table = tableName();
        for (Map<String, Object> stringObjectMap : listDepartment) {
            Map<String, Object> department = connectionMode.attributeAssignment(stringObjectMap, table, departmentOpenApi);
            if (CollUtil.isNotEmpty(department)) {
                referenceTime(zoHoOffset, department, table, false);
                events[0].add(TapSimplify.insertRecordEvent(department, table));
                if (events[0].size() == batchCount) {
                    accept(consumer, events[0], zoHoOffset);
                    events[0] = new ArrayList<>();
                }
            }
            if (!isAlive()) break;
        }
    }

    @Override
    public long batchCount() {
        return departmentOpenApi.getDepartmentCount();
    }

    @Override
    public Object referenceTimeObj(Map<String, Object> data, boolean isStreamRead) {
        return data.get(SchemaLoader.MODIFIED_TIME);
    }
}
