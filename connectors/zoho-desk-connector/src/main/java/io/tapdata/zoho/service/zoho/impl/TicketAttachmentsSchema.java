package io.tapdata.zoho.service.zoho.impl;

import cn.hutool.core.collection.CollUtil;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ZoHoOffset;
import io.tapdata.zoho.service.connectionMode.ConnectionMode;
import io.tapdata.zoho.service.zoho.loader.TicketAttachmentsOpenApi;
import io.tapdata.zoho.service.zoho.schema.Schemas;

import java.util.*;
import java.util.function.BiConsumer;

public class TicketAttachmentsSchema extends Schema implements SchemaLoader {
    TicketAttachmentsOpenApi attachmentsOpenApi;

    @Override
    public String tableName() {
        return Schemas.TicketAttachments.getTableName();
    }

    @Override
    public SchemaLoader configSchema(TapConnectionContext tapConnectionContext) {
        this.attachmentsOpenApi = TicketAttachmentsOpenApi.create(tapConnectionContext);
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
        int pageSize = Math.min(readSize, TicketAttachmentsOpenApi.MAX_PAGE_LIMIT);
        int fromPageIndex = 1;
        TapConnectionContext context = this.attachmentsOpenApi.getContext();
        String modeName = context.getConnectionConfig().getString(CONNECTION_MODE);
        ConnectionMode connectionMode = ConnectionMode.getInstanceByName(context, modeName);
        String tableName = Schemas.TicketAttachments.getTableName();
        String ticketId = "";
        final ZoHoOffset offset = initOffset(offsetState);
        while (isAlive()) {
            List<Map<String, Object>> list = attachmentsOpenApi.page(ticketId, CREATED_TIME, fromPageIndex, pageSize, null, null);
            if (CollUtil.isEmpty(list)) break;
            fromPageIndex += pageSize;
            list.stream().filter(Objects::nonNull).forEach(product -> {
                if (!isAlive()) return;
                Map<String, Object> oneProduct = connectionMode.attributeAssignment(product, tableName, attachmentsOpenApi);
                if (CollUtil.isEmpty(oneProduct)) return;
                acceptOne(oneProduct, offset, isStreamRead, events, readSize, consumer);
            });
        }
        if (events[0].isEmpty()) return;
        accept(consumer, events[0], offsetState);
    }

    @Override
    public Object referenceTimeObj(Map<String, Object> data, boolean isStreamRead) {
        return data.get(MODIFIED_TIME);
    }
}
