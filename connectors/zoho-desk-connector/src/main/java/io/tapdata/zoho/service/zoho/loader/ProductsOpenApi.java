package io.tapdata.zoho.service.zoho.loader;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.HttpEntity;
import io.tapdata.zoho.entity.HttpNormalEntity;
import io.tapdata.zoho.entity.HttpResult;
import io.tapdata.zoho.entity.HttpType;
import io.tapdata.zoho.service.zoho.param.ProductPageParam;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.ZoHoHttp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ProductsOpenApi extends ZoHoStarter implements ZoHoBase {
    public static final String GET_PRODUCT_URL = "/api/v1/products/{product_id}";
    public static final String LIST_PRODUCT_URL = "/api/v1/products";

    protected ProductsOpenApi(TapConnectionContext tapConnectionContext) {
        super(tapConnectionContext);
    }

    public static ProductsOpenApi create(TapConnectionContext tapConnectionContext) {
        return new ProductsOpenApi(tapConnectionContext);
    }

    @Override
    public TapConnectionContext getContext() {
        return this.tapConnectionContext;
    }

    public Map<String, Object> get(String productId) {
        if (Checker.isEmpty(productId)) {
            log.debug("Department Id can not be empty");
        }
        HttpNormalEntity header = requestHeard();
        HttpNormalEntity resetFull = HttpNormalEntity.create().build("product_id", productId);
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL, GET_PRODUCT_URL), HttpType.GET, header).resetFull(resetFull);
        HttpResult httpResult = this.readyAccessToken(http);
        log.debug("Get Product list succeed");
        return parseWithDefault(httpResult.getResult());
    }

    public static final int MAX_PAGE_LIMIT = 100;
    public static final int MIN_PAGE_LIMIT = 1;
    public static final int DEFAULT_PAGE_LIMIT = 10;
    public static final int MIN_FROM = 0;

    private List<Map<String, Object>> page(ProductPageParam param) {
        Integer from = param.getFrom();
        Integer limit = param.getLimit();
        Long deprecated = param.getDeprecated();
        Long departmentId = param.getDepartmentId();
        Long ownerId = param.getOwnerId();
        Long viewId = param.getViewId();
        String sortBy = param.getSortBy();
        String fields = param.getFields();
        List<String> include = param.getInclude();
        HttpEntity form = HttpEntity.create();
        if (Checker.isEmpty(from) || from < MIN_FROM) from = MIN_FROM;
        if (Checker.isEmpty(limit) || limit < MIN_PAGE_LIMIT || limit > MAX_PAGE_LIMIT) limit = DEFAULT_PAGE_LIMIT;
        form.build("from", from);
        form.build("limit", limit);
        if (Checker.isNotEmpty(deprecated)) form.build("deprecated", deprecated);
        if (Checker.isNotEmpty(departmentId)) form.build("departmentId", departmentId);
        if (Checker.isNotEmpty(ownerId)) form.build("ownerId", ownerId);
        if (Checker.isNotEmpty(viewId)) form.build("viewId", viewId);
        if (Checker.isNotEmpty(sortBy)) form.build("sortBy", sortBy);
        if (Checker.isNotEmpty(fields)) form.build("fields", fields);
        if (Checker.isNotEmpty(include)) form.build("include", include);
        return list(form);
    }

    public List<Map<String, Object>> page(Integer from, Integer limit, String sortBy) {
        return page(ProductPageParam.of().withFrom(from).withLimit(limit).withSortBy(sortBy).withDepartmentId(0L));
    }

    private List<Map<String, Object>> list(HttpEntity form) {
        HttpNormalEntity header = requestHeard();
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL, LIST_PRODUCT_URL), HttpType.GET, header).header(header).form(form);
        HttpResult httpResult = this.readyAccessToken(http);
        log.debug("Get product page succeed");
        Object data = parseWithDefault(httpResult.getResult()).get("data");
        return data instanceof List ? (List<Map<String, Object>>) data : new ArrayList<>();
    }

    public enum SortBy {
        /**
         * Sort by a specific attribute : productName, productCode, unitPrice, createdTime or modifiedTime.
         * The default sorting order is ascending.
         * A - prefix denotes descending order of sorting.
         */
        PRODUCT_NAME("productName"),
        PRODUCT_CODE("productCode"),
        UNIT_PRICE("unitPrice"),
        CREATED_TIME("createdTime"),
        MODIFIED_TIME("modifiedTime"),
        ;
        String sortByValue;

        SortBy(String sortBy) {
            this.sortByValue = sortBy;
        }

        public String getSortByValue() {
            return sortByValue;
        }

        public String descSortBy() {
            return "-" + sortByValue;
        }

        void setSortByValue(String sortByValue) {
            this.sortByValue = sortByValue;
        }
    }
}
