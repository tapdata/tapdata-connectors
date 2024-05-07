package io.tapdata.zoho.service.zoho.impl;

import cn.hutool.core.collection.CollUtil;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ZoHoOffset;
import io.tapdata.zoho.service.connection.ConnectionMode;
import io.tapdata.zoho.service.zoho.loader.ContractsOpenApi;
import io.tapdata.zoho.service.zoho.schema.Schemas;

import java.util.*;
import java.util.function.BiConsumer;

public class ContractsSchema extends Schema implements SchemaLoader {
    ContractsOpenApi contractsOpenApi;

    @Override
    public String tableName() {
        return Schemas.Contracts.getTableName();
    }

    @Override
    public SchemaLoader configSchema(TapConnectionContext tapConnectionContext) {
        this.contractsOpenApi = ContractsOpenApi.create(tapConnectionContext);
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
        final List<TapEvent>[] events = new List[]{new ArrayList<>()};
        int pageSize = Math.min(readSize, ContractsOpenApi.MAX_PAGE_LIMIT);
        //从第几个工单开始分页
        int fromPageIndex = 1;
        TapConnectionContext context = this.contractsOpenApi.getContext();
        String modeName = context.getConnectionConfig().getString(CONNECTION_MODE);
        ConnectionMode connectionMode = ConnectionMode.getInstanceByName(context, modeName);
        String tableName = Schemas.Products.getTableName();
        final ZoHoOffset offset = initOffset(offsetState);
        final String sortBy = isBatchRead ? CREATED_TIME : MODIFIED_TIME;
        while (isAlive()) {
            List<Map<String, Object>> list = contractsOpenApi.page(fromPageIndex, pageSize, sortBy);
            if (CollUtil.isEmpty(list)) break;
            fromPageIndex += pageSize;
            list.stream().filter(Objects::nonNull).forEach(product -> {
                Map<String, Object> oneProduct = connectionMode.attributeAssignment(product, tableName, contractsOpenApi);
                if (CollUtil.isEmpty(oneProduct)) return;
                acceptOne(oneProduct, offset, !isBatchRead, events, readSize, consumer);
            });
        }
        if (events[0].isEmpty()) return;
        accept(consumer, events[0], offsetState);
    }
}
