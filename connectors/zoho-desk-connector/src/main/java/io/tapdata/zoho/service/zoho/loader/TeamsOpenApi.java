package io.tapdata.zoho.service.zoho.loader;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.HttpNormalEntity;
import io.tapdata.zoho.entity.HttpResult;
import io.tapdata.zoho.entity.HttpType;
import io.tapdata.zoho.utils.ZoHoHttp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TeamsOpenApi extends ZoHoStarter implements ZoHoBase {
    public static final String LIST_URL = "/api/v1/teams";
    public static final String GET_URL = "/api/v1/teams/{team_id}";

    protected TeamsOpenApi(TapConnectionContext tapConnectionContext) {
        super(tapConnectionContext);
    }

    public static TeamsOpenApi create(TapConnectionContext tapConnectionContext) {
        return new TeamsOpenApi(tapConnectionContext);
    }

    public List<Map<String, Object>> page() {
        HttpNormalEntity header = requestHeard();
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL, LIST_URL), HttpType.GET, header).header(header);
        HttpResult httpResult = this.readyAccessToken(http);
        log.debug("Get all teams succeed");
        Object data = parseWithDefault(httpResult.getResult()).get("teams");
        return data instanceof List ? (List<Map<String, Object>>) data : new ArrayList<>();
    }

    public Map<String, Object> get(String teamId) {
        HttpNormalEntity header = requestHeard();
        HttpNormalEntity resetFull = HttpNormalEntity.create().build("team_id", teamId);
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL, GET_URL), HttpType.GET, header)
                .header(header)
                .resetFull(resetFull);
        HttpResult httpResult = this.readyAccessToken(http);
        log.debug("Get all teams succeed");
        return parseWithDefault(httpResult.getResult());
    }
}
