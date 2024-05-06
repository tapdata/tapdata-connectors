package io.tapdata.zoho.service.zoho.impl;

import cn.hutool.core.collection.CollUtil;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ZoHoOffset;
import io.tapdata.zoho.service.connectionMode.ConnectionMode;
import io.tapdata.zoho.service.zoho.loader.ProductsOpenApi;
import io.tapdata.zoho.service.zoho.schema.Schemas;

import java.util.*;
import java.util.function.BiConsumer;

public class ProductsSchema extends Schema implements SchemaLoader {
    private ProductsOpenApi productsOpenApi;

    @Override
    public String tableName() {
        return Schemas.Products.getTableName();
    }

    @Override
    public SchemaLoader configSchema(TapConnectionContext tapConnectionContext) {
        this.productsOpenApi = ProductsOpenApi.create(tapConnectionContext);
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

    private String sortBy(boolean isBatchRead) {
        return !isBatchRead ? ProductsOpenApi.SortBy.CREATED_TIME.descSortBy() : ProductsOpenApi.SortBy.MODIFIED_TIME.descSortBy();
    }

    public void read(int readSize, Object offsetState, BiConsumer<List<TapEvent>, Object> consumer, boolean isBatchRead) {
        final List<TapEvent>[] events = new List[]{new ArrayList<>()};
        int pageSize = Math.min(readSize, ProductsOpenApi.MAX_PAGE_LIMIT);
        int fromPageIndex = 1;
        TapConnectionContext context = this.productsOpenApi.getContext();
        String modeName = context.getConnectionConfig().getString(CONNECTION_MODE);
        ConnectionMode connectionMode = ConnectionMode.getInstanceByName(context, modeName);
        final ZoHoOffset offset = initOffset(offsetState);
        final String sortBy = sortBy(isBatchRead);
        try {
            while (isAlive()) {
                List<Map<String, Object>> list = productsOpenApi.page(fromPageIndex, pageSize, sortBy);
                if (CollUtil.isEmpty(list)) {
                    break;
                }
                fromPageIndex += pageSize;
                list.stream().filter(Objects::nonNull).forEach(product -> {
                    if (!isAlive()) return;
                    acceptOne(connectionMode, offset, readSize, product, events, isBatchRead, consumer);
                });
            }
        } finally {
            if (!events[0].isEmpty()) {
                accept(consumer, events[0], offsetState);
            }
        }
    }

    private void acceptOne(ConnectionMode connectionMode, ZoHoOffset offset, int readSize, Map<String, Object> product, List<TapEvent>[] events, boolean isBatchRead, BiConsumer<List<TapEvent>, Object> consumer) {
        String tableName = tableName();
        Map<String, Object> oneProduct = connectionMode.attributeAssignment(product, tableName, productsOpenApi);
        if (CollUtil.isNotEmpty(oneProduct)) {
            acceptOne(oneProduct, offset, !isBatchRead, events, readSize, consumer);
        }
    }

    @Override
    public Object referenceTimeObj(Map<String, Object> data, boolean isStreamRead) {
        return isStreamRead ? data.get(MODIFIED_TIME) : null;
    }
}
