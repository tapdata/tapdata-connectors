package io.tapdata.zoho.service.zoho.loader;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.HttpEntity;
import io.tapdata.zoho.entity.HttpNormalEntity;
import io.tapdata.zoho.entity.HttpResult;
import io.tapdata.zoho.entity.HttpType;
import io.tapdata.zoho.service.zoho.param.PageParam;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.ZoHoHttp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ContractsOpenApi extends ZoHoStarter implements ZoHoBase {
    protected ContractsOpenApi(TapConnectionContext tapConnectionContext) {
        super(tapConnectionContext);
    }

    public static ContractsOpenApi create(TapConnectionContext tapConnectionContext) {
        return new ContractsOpenApi(tapConnectionContext);
    }

    public static final String LIST_URL = "api/v1/contracts";
    public static final String GET_URL = "api/v1/contracts/{contract_id}";

    public static final int MAX_PAGE_LIMIT = 100;
    public static final int MIX_PAGE_LIMIT = 1;
    public static final int MIN_PAGE_FROM = 0;
    public static final int DEFAULT_PAGE_LIMIT = 50;

    public List<Map<String, Object>> page(Integer from, Integer limit, String sortBy) {
        return page(PageParam.of().withFrom(from).withLimit(limit).withSortBy(sortBy));
    }

    public List<Map<String, Object>> page(Integer from, Integer limit) {
        return page(PageParam.of().withFrom(from).withLimit(limit));
    }

    public List<Map<String, Object>> page(PageParam pageParam) {
        HttpEntity form = HttpEntity.create();
        Integer from = pageParam.getFrom();
        Integer limit = pageParam.getLimit();
        String viewId = pageParam.getViewId();
        String departmentId = pageParam.getDepartmentId();
        String accountId = pageParam.getAccountId();
        String sortBy = pageParam.getSortBy();
        String ownerId = pageParam.getOwnerId();
        String contractName = pageParam.getContractName();
        String include = pageParam.getInclude();
        if (Checker.isNotEmpty(viewId)) form.build("viewId", viewId);
        if (Checker.isNotEmpty(departmentId)) form.build("departmentId", departmentId);
        if (Checker.isNotEmpty(accountId)) form.build("accountId", accountId);
        if (Checker.isNotEmpty(sortBy)) form.build("sortBy", sortBy);
        if (Checker.isNotEmpty(ownerId)) form.build("ownerId", ownerId);
        if (Checker.isNotEmpty(contractName)) form.build("contractName", contractName);
        if (Checker.isNotEmpty(include)) form.build("include", include);
        if (Checker.isEmpty(from) || from < MIN_PAGE_FROM) from = MIN_PAGE_FROM;
        if (Checker.isEmpty(limit) || limit < MIX_PAGE_LIMIT || limit > MAX_PAGE_LIMIT) limit = DEFAULT_PAGE_LIMIT;
        return page(form.build("from", from).build("limit", limit));
    }

    private List<Map<String, Object>> page(HttpEntity form) {
        HttpNormalEntity header = requestHeard();
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL, LIST_URL), HttpType.GET, header).header(header).form(form);
        HttpResult httpResult = this.readyAccessToken(http);
        log.debug("Get Contracts page succeed");
        Object data = parseWithDefault(httpResult.getResult()).get("data");
        List<Map<String, Object>> result = new ArrayList<>();
        if (data instanceof List) {
            result.addAll((List<Map<String, Object>>) data);
        }
        return result;
    }

    public Map<String, Object> get(String contractId) {
        HttpNormalEntity header = requestHeard();
        HttpNormalEntity resetFull = HttpNormalEntity.create().build("contract_id", contractId);
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL, GET_URL), HttpType.GET, header).header(header).resetFull(resetFull);
        HttpResult httpResult = this.readyAccessToken(http);
        log.debug("Get Contract info succeed");
        Object result = httpResult.getResult();
        return Checker.isEmpty(result) ? Collections.emptyMap() : (Map<String, Object>) result;
    }
}
