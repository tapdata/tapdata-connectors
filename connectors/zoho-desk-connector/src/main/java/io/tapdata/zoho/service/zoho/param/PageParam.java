package io.tapdata.zoho.service.zoho.param;

public class PageParam {
    Integer from;
    Integer limit;
    String viewId;
    String departmentId;
    String accountId;
    String sortBy;
    String ownerId;
    String contractName;
    String include;
    public static PageParam of() {
        return new PageParam();
    }
    public PageParam withFrom(Integer from) {
        this.from = from;
        return this;
    }
    public PageParam withLimit(Integer limit) {
        this.limit = limit;
        return this;
    }
    public PageParam withViewId(String viewId) {
        this.viewId = viewId;
        return this;
    }
    public PageParam withDepartmentId(String departmentId) {
        this.departmentId = departmentId;
        return this;
    }
    public PageParam withAccountId(String accountId) {
        this.accountId = accountId;
        return this;
    }
    public PageParam withSortBy(String sortBy) {
        this.sortBy = sortBy;
        return this;
    }
    public PageParam withOwnerId(String ownerId) {
        this.ownerId = ownerId;
        return this;
    }
    public PageParam withContractName(String contractName) {
        this.contractName = contractName;
        return this;
    }
    public PageParam withInclude(String include) {
        this.include = include;
        return this;
    }

    public Integer getFrom() {
        return from;
    }

    public Integer getLimit() {
        return limit;
    }

    public String getViewId() {
        return viewId;
    }

    public String getDepartmentId() {
        return departmentId;
    }

    public String getAccountId() {
        return accountId;
    }

    public String getSortBy() {
        return sortBy;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public String getContractName() {
        return contractName;
    }

    public String getInclude() {
        return include;
    }
}
