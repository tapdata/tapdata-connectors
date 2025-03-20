package io.tapdata.connector.starrocks.bean;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StarrocksConfigTest {
    @DisplayName("test when useHTTPS is false")
    @Test
    void test1(){
        StarrocksConfig StarrocksConfig = new StarrocksConfig();
        Map<String,Object> dataMap=new HashMap<>();
        dataMap.put("useHTTPS",false);
        dataMap.put("StarrocksHttp","http://localhost:8080");
        StarrocksConfig.load(dataMap);
        assertEquals("localhost:8080",StarrocksConfig.getStarrocksHttp());
    }
    @DisplayName("test when useHTTP is true")
    @Test
    void test2(){
        StarrocksConfig StarrocksConfig = new StarrocksConfig();
        Map<String,Object> dataMap=new HashMap<>();
        dataMap.put("useHTTPS",true);
        dataMap.put("StarrocksHttp","https://localhost:8080");
        StarrocksConfig.load(dataMap);
        assertEquals("localhost:8080",StarrocksConfig.getStarrocksHttp());
    }
}
