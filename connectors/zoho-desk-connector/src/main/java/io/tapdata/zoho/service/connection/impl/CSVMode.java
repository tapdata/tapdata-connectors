package io.tapdata.zoho.service.connection.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.json.JSONObject;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ContextConfig;
import io.tapdata.zoho.enums.Constants;
import io.tapdata.zoho.enums.FieldModelType;
import io.tapdata.zoho.service.connection.ConnectionMode;
import io.tapdata.zoho.service.zoho.loader.OrganizationFieldLoader;
import io.tapdata.zoho.service.zoho.loader.TicketLoader;
import io.tapdata.zoho.service.zoho.loader.ZoHoBase;
import io.tapdata.zoho.service.zoho.schema.Schema;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.MapUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.tapdata.base.ConnectorBase.*;

/**
 * {
 * "label": "${csv}",
 * "value": "CSVMode"
 * }
 */
public class CSVMode implements ConnectionMode {
    TapConnectionContext connectionContext;
    ContextConfig contextConfig;

    @Override
    public TapConnectionContext getConnectionContext() {
        return connectionContext;
    }

    @Override
    public Log getLog() {
        return connectionContext.getLog();
    }

    @Override
    public ConnectionMode config(TapConnectionContext connectionContext) {
        this.connectionContext = connectionContext;
        return this;
    }

    @Override
    public List<TapTable> discoverSchemaV1(List<String> tables, int tableSize) {
        if (CollUtil.isEmpty(tables)) return new ArrayList<>();
        TapTable tapTable = table("Tickets")
                .add(field(ID, LONG).isPrimaryKey(true).primaryKeyPos(1))    //"Ticket Reference Id"
                .add(field("ticketNumber", LONG))
                .add(field("departmentName", STRING_NORMAL))    //"Department"     department（需要从分页中需添加到详情） ->  department.name（掉方法打平）
                .add(field("departmentId", LONG))
                .add(field("contactAccountAccountName", STRING_NORMAL))//contact      contact（需要从分页中需添加到详情）->contact.account.accountName（掉方法打平）
                .add(field("contactAccountId", LONG))//contactID      contact（需要从分页中需添加到详情）->contact.account.id（掉方法打平）
                .add(field("contactLastName", STRING_NORMAL))//联系人名称      contact（需要从分页中需添加到详情）->contact.lastName（掉方法打平）
                .add(field("contactId", LONG))//联系人名称 Id      contact（需要从分页中需添加到详情）->contact.id（掉方法打平）
                .add(field("email", EMAIL))
                .add(field("phone", PHONE))
                .add(field("subject", STRING_NORMAL))
                .add(field("description", TEXTAREA))
                .add(field("status", STRING_MINOR))
                .add(field("productName", STRING_NORMAL))//产品名称
                .add(field("productId", LONG))//产品名称 Id
                .add(field("assigneeName", STRING_NORMAL))//工单所有者   assignee（需要从分页中需添加到详情）->assignee.lastName（掉方法打平）
                .add(field("assigneeId", LONG))//工单所有者 Id    assignee（需要从分页中需添加到详情）->assignee.id（掉方法打平）
                .add(field("createdByName", STRING_NORMAL))
                .add(field("createdBy", LONG))
                .add(field("modifiedByName", STRING_NORMAL))
                .add(field("modifiedBy", LONG))
                .add(field("createdTime", DATE_TIME))
                .add(field("modifiedTime", DATE_TIME))
                .add(field("closedTime", DATE_TIME))
                .add(field("dueDate", DATE_TIME))
                .add(field("priority", STRING_MINOR))
                .add(field("channel", STRING_MINOR))//Model
                .add(field("isOverDue", BOOLEAN))
                .add(field("isEscalated", BOOLEAN))
                .add(field("classification", STRING_NORMAL))
                .add(field("resolution", STRING_NORMAL))//object
                .add(field("category", STRING_NORMAL))//object
                .add(field("subCategory", STRING_NORMAL))//object
                .add(field("customerResponseTime", DATE_TIME))
                .add(field("teamId", LONG))
                .add(field("teamName", STRING_NORMAL))//object工单所有者 Id    team（需要从分页中需添加到详情）->team.name  （掉方法打平）
                .add(field("tags", STRING_NORMAL))
                .add(field("language", STRING_MINOR))
                .add(field("timeEntryCount", INTEGER))// 工单搁置时间

//                    .add(field("statusType","String"))
//                    .add(field("onholdTime","Object"))
//                    .add(field("source","Map"))
//                    .add(field("sharedDepartments","JAVA_Array"))
//                    .add(field("approvalCount",INTEGER))
//                    .add(field("isTrashed",BOOLEAN))
//                    .add(field("isResponseOverdue",BOOLEAN))
//                    .add(field("contactId",LONG))
//                    .add(field("threadCount",INTEGER))
//                    .add(field("secondaryContacts","JAVA_Array"))
//                    .add(field("commentCount",INTEGER))
//                    .add(field("taskCount",INTEGER))
//                    .add(field("accountId",LONG))
//                    .add(field("webUrl","String"))
//                    .add(field("isSpam",BOOLEAN))
//                    .add(field("entitySkills","JAVA_Array"))
//                    .add(field("sentiment","Object"))
//                    .add(field("customFields","Map"))
//                    .add(field("isArchived",BOOLEAN))
//                    .add(field("channelRelatedInfo","Object"))
//                    .add(field("responseDueDate","Object"))
//                    .add(field("isDeleted",BOOLEAN))
//                    .add(field("modifiedBy",LONG))
//                    .add(field("followerCount",INTEGER))
//                    .add(field("layoutDetails","Map"))
//                    .add(field("channelCode","Object"))
//                    .add(field("isFollowing",BOOLEAN))
//                    .add(field("cf","Map"))
//                    .add(field("slaId",LONG))
//                    .add(field("layoutId",LONG))
//                    .add(field("assigneeId",LONG))
//                    .add(field("createdBy",LONG))
//                    .add(field("tagCount",INTEGER))
//                    .add(field("attachmentCount",INTEGER))

                ;
        // 查询自定义属性列表
        Map<String, Map<String, Object>> customFieldMap = OrganizationFieldLoader.create(connectionContext)
                .customFieldMap(FieldModelType.TICKETS);
        //accessToken过期 
        if (CollUtil.isEmpty(customFieldMap)) {
            throw new CoreException("Get custom fields failed, custom fields map is empty");
        }
        customFieldMap.forEach((fieldName, field) -> {
            if (field instanceof JSONObject) {
                Object filedName = field.get(NAME);
                if (Checker.isNotEmpty(filedName)) {
                    // 根据type属性匹配对应tapdata类型
                    Object fieldTypeObj = field.get(TYPE);
                    tapTable.add(field(
                            Constants.CUSTOM_FIELD_SUFFIX + filedName,
                            Checker.isEmpty(fieldTypeObj) ? NULL : (String) fieldTypeObj));
                }
            }
        });
        return list(tapTable);
    }

    public Map<String, Object> attributeAssignmentSelfV2(Map<String, Object> ticketDetail) {
        Map<String, Object> ticketCSVDetail = new HashMap<>();
        //加入自定义属性
        Map<String, Object> customFieldMap = this.setCustomField(ticketDetail);
        if (Checker.isNotEmpty(customFieldMap)) {
            ticketCSVDetail.putAll(customFieldMap);
        }
        //O 统计需要操作的属性,子属性用点分割；数据组结果会以|分隔返回，大文本会以""包裹
        MapUtil.putMapSplitByDotKeyNameFirstUpper(ticketDetail, ticketCSVDetail,
                ID,    //"Ticket Reference Id"
                "ticketNumber",
                "department.name",//"Department"     department（需要从分页中需添加到详情） ->  department.name（掉方法打平）
                "departmentId",
                "contact.account.accountName",//contact      contact（需要从分页中需添加到详情）->contact.account.accountName（掉方法打平）
                "contact.account.id",//contactID      contact（需要从分页中需添加到详情）->contact.account.id（掉方法打平）
                "contact.lastName",//联系人名称      contact（需要从分页中需添加到详情）->contact.lastName（掉方法打平）
                "contact.id",//联系人名称 Id      contact（需要从分页中需添加到详情）->contact.id（掉方法打平）
                "email",
                "phone",
                "subject",
                "description",
                "status",
                "productName",//产品名称
                "productId",//产品名称 Id
                "assignee.lastName",//工单所有者   assignee（需要从分页中需添加到详情）->assignee.lastName（掉方法打平）
                "assignee.id",//工单所有者 Id    assignee（需要从分页中需添加到详情）->assignee.id（掉方法打平）
                "createdByName",
                "createdBy",
                "modifiedByName",
                "modifiedBy",
                "createdTime",
                "modifiedTime",
                "closedTime",
                "dueDate",
                "priority",
                "channel",
                "isOverDue",
                "isEscalated",
                "classification",
                "resolution",
                "category",
                "subCategory",
                "customerResponseTime",
                "teamId",
                "teamName",//工单所有者 Id    team（需要从分页中需添加到详情）->team.name  （掉方法打平）
                "tags",
                "language",
                "timeEntryCount"// 工单搁置时间
        );
        this.removeJsonNull(ticketCSVDetail);
        return ticketCSVDetail;
    }

    @Override
    public Map<String, Object> attributeAssignmentSelf(Map<String, Object> objectMap, String tableName) {
        return Schema.schema(tableName).attributeAssignmentSelfCsv(objectMap, contextConfig);
    }

    public Map<String, Object> attributeAssignmentV2(Map<String, Object> stringObjectMap, String tableName) {
        Object ticketIdObj = stringObjectMap.get(ID);
        if (Checker.isEmpty(ticketIdObj)) {
            getLog().debug("Ticket Id can not be empty");
            return null;
        }
        TicketLoader ticketLoader = TicketLoader.create(connectionContext);
        Map<String, Object> ticketDetail = ticketLoader.getOne((String) ticketIdObj);
        //把分页结果中具有但详情结果中不具有切CSV结构数据需要的结构进行提取
        ticketDetail.put(DEPARTMENT, stringObjectMap.get(DEPARTMENT));
        ticketDetail.put(CONTACT, stringObjectMap.get(CONTACT));
        ticketDetail.put(ASSIGNEE, stringObjectMap.get(ASSIGNEE));
        return this.attributeAssignmentSelf(ticketDetail, tableName);
    }

    @Override
    public Map<String, Object> attributeAssignment(Map<String, Object> stringObjectMap, String tableName, ZoHoBase openApi) {
        return Schema.schema(tableName).config(openApi).attributeAssignmentCsv(stringObjectMap, connectionContext, contextConfig);
    }

    //CustomFields
    private Map<String, Object> setCustomField(Map<String, Object> stringObjectMap) {
        Map<String, Object> result = new HashMap<>();
        Object customFieldsObj = stringObjectMap.get("cf");
        if (null == customFieldsObj) {
            return result;
        }
        Map<String, Object> customFields = (Map<String, Object>) customFieldsObj;
        if (customFields.isEmpty()) {
            return result;
        }
        customFields.forEach(result::put);
        if (!result.isEmpty()) {
            stringObjectMap.putAll(result);
        }
        return result;
    }
}
