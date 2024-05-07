package io.tapdata.zoho.service.zoho.impl;

import cn.hutool.core.collection.CollUtil;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ContextConfig;
import io.tapdata.zoho.entity.HttpEntity;
import io.tapdata.zoho.entity.ZoHoOffset;
import io.tapdata.zoho.service.connection.ConnectionMode;
import io.tapdata.zoho.service.zoho.loader.TicketLoader;
import io.tapdata.zoho.service.zoho.schema.Schemas;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class TicketsSchema extends Schema implements SchemaLoader {
    TicketLoader ticketLoader;
    private static final int BATCH_READ_MAX_PAGE_SIZE = 100;//ZoHo ticket page size 1~100
    protected static final String SORT_BY = "sortBy";
    protected static final String INCLUDE = "include";
    protected static final String FIELDS = "fields";
    protected static final String ID = "id";
    protected static final String LIMIT = "limit";
    protected static final String FROM = "from";
    protected static final String CF = "cf";

    @Override
    public String tableName() {
        return Schemas.Tickets.getTableName();
    }

    @Override
    public TicketsSchema configSchema(TapConnectionContext context) {
        this.ticketLoader = TicketLoader.create(context);
        return this;
    }

    @Override
    public void streamRead(Object offsetState, int recordSize, StreamReadConsumer consumer) {
        throw new CoreException("Table Ticket not support stream read");
    }

    @Override
    public void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) {
        Long readEnd = System.currentTimeMillis();
        ZoHoOffset zoHoOffset = new ZoHoOffset();
        //current read end as next read begin
        Map<String, Long> offsetState = new HashMap<>();
        offsetState.put(Schemas.Tickets.getTableName(), readEnd);
        zoHoOffset.setTableUpdateTimeMap(offsetState);
        read(batchCount, zoHoOffset, consumer, false);
    }

    @Override
    public long batchCount() {
        return ticketLoader.count();
    }

    private void buildParam(HttpEntity tickPageParam, ContextConfig contextConfig, boolean isStreamRead) {
        tickPageParam.build(SORT_BY, (contextConfig.sortType() ? "-" : "") + (isStreamRead ? MODIFIED_TIME : CREATED_TIME));
        tickPageParam.build(INCLUDE, "contacts,products,departments,team,isRead,assignee");
    }

    protected void doIfNeedDetail(boolean finalNeedDetail, String fields, HttpEntity tickPageParam, Map<String, List<Map<String, Object>>> idGroupOfCf) {
        if (finalNeedDetail) {
            return;
        }
        Optional.ofNullable(fields).ifPresent(f -> tickPageParam.build(FIELDS, f));
        List<Map<String, Object>> cfList = ticketLoader.list(tickPageParam);
        tickPageParam.remove(FIELDS);
        idGroupOfCf.putAll(cfList.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(x -> String.valueOf(x.get(ID)))));
    }

    /**
     * 分页读取事项列表，并依次查询事项详情
     *
     * @param readSize
     * @param consumer ZoHo提供的分页查询条件
     *                 -from                     integer                 Index number, starting from which the tickets must be fetched
     *                 -limit                    integer[1-100]          Number of tickets to fetch  ,pageSize
     *                 -departmentId             long                    ID of the department from which the tickets must be fetched (Please note that this key will be deprecated soon and replaced by the departmentIds key.)
     *                 -departmentIds            List<Long>              Departments from which the tickets need to be queried'
     *                 -viewId                   long                    ID of the view to apply while fetching the resources
     *                 -assignee                 string(MaxLen:100)      assignee - Key that filters tickets by assignee. Values allowed are Unassigned or a valid assigneeId. Multiple assigneeIds can be passed as comma-separated values.
     *                 -channel                  string(MaxLen:100)      Filter by channel through which the tickets originated. You can include multiple values by separating them with a comma
     *                 -status                   string(MaxLen:100)      Filter by resolution status of the ticket. You can include multiple values by separating them with a comma
     *                 -sortBy                   string(MaxLen:100)      Sort by a specific attribute: responseDueDate or customerResponseTime or createdTime or ticketNumber. The default sorting order is ascending. A - prefix denotes descending order of sorting.
     *                 -receivedInDays           integer                 Fetches recent tickets, based on customer response time. Values allowed are 15, 30 , 90.
     *                 -include                  string(MaxLen:100)      Additional information related to the tickets. Values allowed are: contacts, products, departments, team, isRead and assignee. You can pass multiple values by separating them with commas in the API request.
     *                 -fields                   string(MaxLen:100)      Key that returns the values of mentioned fields (both pre-defined and custom) in your portal. All field types except multi-text are supported. Standard, non-editable fields are supported too. These fields include: statusType, webUrl, layoutId. Maximum of 30 fields is supported as comma separated values.
     *                 -priority                 string(MaxLen:100)      Key that filters tickets by priority. Multiple priority levels can be passed as comma-separated values.
     */
    public void read(int readSize, Object offsetState, BiConsumer<List<TapEvent>, Object> consumer, boolean isStreamRead) {
        final List<TapEvent>[] events = new List[]{new ArrayList<>()};
        int pageSize = Math.min(readSize, BATCH_READ_MAX_PAGE_SIZE);

        TapConnectionContext context = this.ticketLoader.getContext();
        ContextConfig contextConfig = ticketLoader.veryContextConfigAndNodeConfig();
        HttpEntity tickPageParam = ticketLoader.getTickPageParam().build(LIMIT, pageSize);
        buildParam(tickPageParam, contextConfig, isStreamRead);

        //从第几个工单开始分页
        int fromPageIndex = 1;
        String modeName = context.getConnectionConfig().getString(CONNECTION_MODE);
        ConnectionMode connectionMode = ConnectionMode.getInstanceByName(context, modeName);
        String tableName = Schemas.Tickets.getTableName();
        final ZoHoOffset offset = initOffset(offsetState);
        boolean finalNeedDetail = contextConfig.needDetailObj();
        String fields = contextConfig.fields();

        try {
            while (isAlive()) {
                tickPageParam.build(FROM, fromPageIndex);
                List<Map<String, Object>> list = ticketLoader.list(tickPageParam);
                if (CollUtil.isEmpty(list)) {
                    break;
                }
                Map<String, List<Map<String, Object>>> idGroupOfCf = new HashMap<>();
                doIfNeedDetail(finalNeedDetail, fields, tickPageParam, idGroupOfCf);
                fromPageIndex += pageSize;
                list.stream().filter(Objects::nonNull).forEach(ticket -> {
                    Object id = ticket.get(ID);
                    List<Map<String, Object>> cfMaps = idGroupOfCf.get(String.valueOf(id));
                    boolean thisRecordNeedDetail = cfMap(ticket, cfMaps, finalNeedDetail);
                    Map<String, Object> oneTicket = initOneTicket(connectionMode, ticket, tableName, thisRecordNeedDetail, finalNeedDetail);
                    if (CollUtil.isEmpty(oneTicket)) return;
                    acceptOne(oneTicket, offset, isStreamRead, events, readSize, consumer);
                });
            }
        } finally {
            if (!events[0].isEmpty()) {
                accept(consumer, events[0], offset);
            }
        }
    }

    protected Map<String, Object> initOneTicket(ConnectionMode connectionMode, Map<String, Object> ticket, String tableName, boolean thisRecordNeedDetail, boolean finalNeedDetail) {
        return finalNeedDetail || thisRecordNeedDetail ?
                connectionMode.attributeAssignment(ticket, tableName, ticketLoader)
                : ticket;
    }

    private boolean cfMap(Map<String, Object> ticket, List<Map<String, Object>> cfMaps, boolean finalNeedDetail) {
        boolean thisRecordNeedDetail = null == cfMaps || cfMaps.isEmpty();
        if (!finalNeedDetail && !thisRecordNeedDetail) {
            for (Map<String, Object> cfMap : cfMaps) {
                if (null != cfMap && !cfMap.isEmpty() && null != cfMap.get(CF)) {
                    ticket.putAll(cfMap);
                }
            }
        }
        return thisRecordNeedDetail;
    }
}
