package io.tapdata.connector.doris.bean;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DorisConfigTest {
    @DisplayName("test when useHTTPS is false")
    @Test
    void test1(){
        DorisConfig dorisConfig = new DorisConfig();
        Map<String,Object> dataMap=new HashMap<>();
        dataMap.put("useHTTPS",false);
        dataMap.put("dorisHttp","http://localhost:8080");
        dorisConfig.load(dataMap);
        assertEquals("localhost:8080",dorisConfig.getDorisHttp());
    }
    @DisplayName("test when useHTTP is true")
    @Test
    void test2(){
        DorisConfig dorisConfig = new DorisConfig();
        Map<String,Object> dataMap=new HashMap<>();
        dataMap.put("useHTTPS",true);
        dataMap.put("dorisHttp","https://localhost:8080");
        dorisConfig.load(dataMap);
        assertEquals("localhost:8080",dorisConfig.getDorisHttp());
    }
}
