package io.tapdata.mongodb;

import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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



}
