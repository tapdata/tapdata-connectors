package io.tapdata.zoho.service.zoho.param;

import java.util.List;

public class ProductPageParam {
    Integer from;
    Integer limit;
    Long deprecated;
    Long departmentId;
    Long ownerId;
    Long viewId;
    String sortBy;
    String fields;
    List<String> include;

    public static ProductPageParam of() {
        return new ProductPageParam();
    }

    public ProductPageParam withFrom(Integer from) {
        this.from = from;
        return this;
    }

    public ProductPageParam withLimit(Integer limit) {
        this.limit = limit;
        return this;
    }

    public ProductPageParam withDeprecated(Long deprecated) {
        this.deprecated = deprecated;
        return this;
    }

    public ProductPageParam withDepartmentId(Long departmentId) {
        this.departmentId = departmentId;
        return this;
    }

    public ProductPageParam withOwnerId(Long ownerId) {
        this.ownerId = ownerId;
        return this;
    }

    public ProductPageParam withViewId(Long viewId) {
        this.viewId = viewId;
        return this;
    }

    public ProductPageParam withSortBy(String sortBy) {
        this.sortBy = sortBy;
        return this;
    }

    public ProductPageParam withFields(String fields) {
        this.fields = fields;
        return this;
    }

    public ProductPageParam withInclude(List<String> include) {
        this.include = include;
        return this;
    }

    public Integer getFrom() {
        return from;
    }

    public Integer getLimit() {
        return limit;
    }

    public Long getDeprecated() {
        return deprecated;
    }

    public Long getDepartmentId() {
        return departmentId;
    }

    public Long getOwnerId() {
        return ownerId;
    }

    public Long getViewId() {
        return viewId;
    }

    public String getSortBy() {
        return sortBy;
    }

    public String getFields() {
        return fields;
    }

    public List<String> getInclude() {
        return include;
    }
}