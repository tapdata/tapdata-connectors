package io.tapdata.mongodb;

import io.tapdata.entity.schema.value.DateTime;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

public class ExecuteObjectTest {

    @Test
    public void testGetFilterByString(){
        Map<String, Object> executeObj = new LinkedHashMap<>();
        String str = "{name:1111}";
        executeObj.put("filter",str);
        ExecuteObject executeObject = new ExecuteObject(executeObj);
        Document actualData = executeObject.getFilter();

        int exceptedDataValue =1111;
        Assertions.assertEquals(exceptedDataValue,actualData.get("name"));

    }

    @Test
    void operationObjectShouldConvertDateTimeToMongoDate() {
        long epochMilli = 1_715_187_045_123L;
        Map<String, Object> set = new LinkedHashMap<>();
        set.put("salesDateTime", new DateTime(Instant.ofEpochMilli(epochMilli)));
        Map<String, Object> operationObject = new LinkedHashMap<>();
        operationObject.put("$set", set);
        Map<String, Object> executeObjectMap = new LinkedHashMap<>();
        executeObjectMap.put("opObject", operationObject);

        ExecuteObject executeObject = new ExecuteObject(executeObjectMap);

        Map<?, ?> convertedSet = (Map<?, ?>) executeObject.getOpObject().get("$set");
        Assertions.assertEquals(new Date(epochMilli), convertedSet.get("salesDateTime"));
    }



}
