package io.tapdata.zoho.service.connection.impl;

import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.service.connection.ConnectionMode;
import io.tapdata.zoho.service.zoho.loader.TicketLoader;
import io.tapdata.zoho.service.zoho.loader.ZoHoBase;
import io.tapdata.zoho.service.zoho.schema.Schema;
import io.tapdata.zoho.utils.Checker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.*;

public class DocumentMode implements ConnectionMode {
    TapConnectionContext connectionContext;

    @Override
    public ConnectionMode config(TapConnectionContext connectionContext) {
        this.connectionContext = connectionContext;
        return this;
    }

    @Override
    public Log getLog() {
        return connectionContext.getLog();
    }

    @Override
    public TapConnectionContext getConnectionContext() {
        return connectionContext;
    }

    @Override
    public List<TapTable> discoverSchemaV1(List<String> tables, int tableSize) {
        if (tables != null && !tables.isEmpty()) {
            return new ArrayList<>();
        }
        return list(
                table("Tickets")
                        .add(field("id", LONG).isPrimaryKey(true).primaryKeyPos(1))
                        .add(field("modifiedTime", DATE_TIME))
                        .add(field("description", TEXTAREA))
                        .add(field("subCategory", MAP))//Object
                        .add(field("statusType", STRING_MINOR))
                        .add(field("subject", STRING_NORMAL))
                        .add(field("dueDate", DATE_TIME))
                        .add(field("departmentId", LONG))
                        .add(field("channel", STRING_MINOR))
                        .add(field("onholdTime", DATE_TIME))//Object
                        .add(field("language", STRING_MINOR))
                        .add(field("source", MAP))
                        .add(field("resolution", STRING_NORMAL))//Object
                        .add(field("sharedDepartments", JAVA_ARRAY))
                        .add(field("closedTime", DATE_TIME))//Object
                        .add(field("approvalCount", INTEGER))
                        .add(field("isOverDue", BOOLEAN))//Object
                        .add(field("isTrashed", BOOLEAN))//Boolean
                        .add(field("contact", MAP))
                        .add(field("createdTime", DATE_TIME))
                        .add(field("isResponseOverdue", BOOLEAN))//Object
                        .add(field("customerResponseTime", DATE_TIME))
                        .add(field("productId", LONG))//Object
                        .add(field("contactId", LONG))
                        .add(field("threadCount", INTEGER))
                        .add(field("secondaryContacts", JAVA_ARRAY))
                        .add(field("priority", STRING_MINOR))
                        .add(field("classification", STRING_NORMAL))
                        .add(field("commentCount", INTEGER))
                        .add(field("taskCount", INTEGER))
                        .add(field("accountId", LONG))//Object
                        .add(field("phone", PHONE))
                        .add(field("webUrl", URL))
                        .add(field("assignee", MAP))
                        .add(field("isSpam", BOOLEAN))//Object
                        .add(field("status", STRING_MINOR))
                        .add(field("entitySkills", JAVA_ARRAY))
                        .add(field("ticketNumber", INTEGER))
                        .add(field("sentiment", MAP))//Object
                        .add(field("customFields", MAP))
                        .add(field("isArchived", BOOLEAN))//Object
                        .add(field(TEXTAREA, TEXTAREA))
                        .add(field("timeEntryCount", INTEGER))
                        .add(field("channelRelatedInfo", MAP))//Object
                        .add(field("responseDueDate", DATE))//Object
                        .add(field("isDeleted", BOOLEAN))//Object
                        .add(field("modifiedBy", LONG))
                        .add(field("department", MAP))
                        .add(field("followerCount", INTEGER))
                        .add(field("email", EMAIL))
                        .add(field("layoutDetails", MAP))
                        .add(field("channelCode", STRING_MINOR))//Object
                        .add(field("product", MAP))//Object
                        .add(field("isFollowing", BOOLEAN))//Object
                        .add(field("cf", MAP))
                        .add(field("slaId", LONG))
                        .add(field("team", MAP))//Object
                        .add(field("layoutId", LONG))
                        .add(field("assigneeId", LONG))
                        .add(field("createdBy", LONG))
                        .add(field("teamId", LONG))//Object
                        .add(field("tagCount", INTEGER))
                        .add(field("attachmentCount", INTEGER))
                        .add(field("isEscalated", BOOLEAN))//Object
                        .add(field("category", MAP))//Object
                        .add(field("department", MAP))
                        .add(field("contact", MAP))//Object
                        .add(field("assignee", MAP))//Object
        );
    }

    @Override
    public Map<String, Object> attributeAssignmentSelf(Map<String, Object> ticketDetail, String tableName) {
        this.removeJsonNull(ticketDetail);
        return ticketDetail;
    }

    public Map<String, Object> attributeAssignmentSelfV2(Map<String, Object> obj, String tableName) {
        return Schema.schema(tableName).attributeAssignmentSelfDocument(obj);
    }

    public Map<String, Object> attributeAssignmentV2(Map<String, Object> stringObjectMap, String tableName) {
        Object ticketIdObj = stringObjectMap.get(ID);
        if (Checker.isEmpty(ticketIdObj)) {
            getLog().debug("Ticket Id can not be empty");
        }
        Map<String, Object> ticketDetail = TicketLoader.create(connectionContext).getOne((String) ticketIdObj);
        ticketDetail.put(DEPARTMENT, stringObjectMap.get(DEPARTMENT));
        ticketDetail.put(CONTACT, stringObjectMap.get(CONTACT));
        ticketDetail.put(ASSIGNEE, stringObjectMap.get(ASSIGNEE));
        return this.attributeAssignmentSelf(ticketDetail, tableName);
    }

    @Override
    public Map<String, Object> attributeAssignment(Map<String, Object> stringObjectMap, String tableName, ZoHoBase openApi) {
        return Schema.schema(tableName).config(openApi).attributeAssignmentDocument(stringObjectMap, connectionContext);
    }
}
