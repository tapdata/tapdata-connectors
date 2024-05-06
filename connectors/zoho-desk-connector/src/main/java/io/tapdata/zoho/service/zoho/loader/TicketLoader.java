package io.tapdata.zoho.service.zoho.loader;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.zoho.entity.HttpEntity;
import io.tapdata.zoho.entity.HttpNormalEntity;
import io.tapdata.zoho.entity.HttpResult;
import io.tapdata.zoho.entity.HttpType;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.ZoHoHttp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TicketLoader extends ZoHoStarter implements ZoHoBase {
    public static final String GET_COUNT_URL = "/api/v1/ticketsCount";

    public TapConnectionContext getContext() {
        return this.tapConnectionContext;
    }

    private TicketLoader(TapConnectionContext tapConnectionContext) {
        super(tapConnectionContext);
    }

    public static TicketLoader create(TapConnectionContext tapConnectionContext) {
        return new TicketLoader(tapConnectionContext);
    }

    /**
     * 根据工单ID获取一个工单详情
     */
    public Map<String, Object> getOne(String ticketId) {
        if (Checker.isEmpty(ticketId)) {
            log.debug("Ticket Id can not be null or not be empty.");
        }
        String url = "/api/v1/tickets/{ticketID}";
        HttpNormalEntity header = requestHeard();
        HttpNormalEntity resetFull = HttpNormalEntity.create().build("ticketID", ticketId);
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL, url), HttpType.GET, header).resetFull(resetFull);
        HttpResult httpResult = this.readyAccessToken(http);
        log.debug("Get ticket list succeed.");
        return parseWithDefault(httpResult.getResult());
    }

    /**
     * 获取查询条件下的全部工单列表
     */
    public List<Map<String, Object>> list(HttpEntity form) {
        String url = "/api/v1/tickets";
        HttpNormalEntity header = requestHeard();
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL, url), HttpType.GET, header).header(header).form(form);
        HttpResult httpResult = this.readyAccessToken(http);
        log.debug("Get ticket list succeed.");
        Map<String, Object> map = parseWithDefault(httpResult.getResult());
        Object data = map.get("data");
        return Checker.isEmpty(data) ? new ArrayList<>() : (List<Map<String, Object>>) data;
    }

    /**
     * 获取查询条件下的全部工单数
     */
    public Integer count() {
        HttpNormalEntity header = requestHeard();
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL, GET_COUNT_URL), HttpType.GET, header)
                .header(header);
        try {
            HttpResult httpResult = this.readyAccessToken(http);
            Object result = httpResult.getResult();
            if (result instanceof Map) {
                Object count = ((Map<String, Object>)result).get("count");
                int c = Checker.isEmpty(count) ? 0 : (int) count;
                log.debug("Get ticket count succeed, count: {}", c);
                return c;
            }
            log.warn("Get ticket count failed.");
            return 0;
        } catch (Exception e) {
            return 1;
        }
    }

    public HttpEntity getTickPageParam() {
        HttpEntity form = HttpEntity.create();
        if (Checker.isNotEmpty(this.tapConnectionContext) && this.tapConnectionContext instanceof TapConnectorContext) {
            return form.build("limit", 100);
        } else {
            return form;
        }
    }
}
