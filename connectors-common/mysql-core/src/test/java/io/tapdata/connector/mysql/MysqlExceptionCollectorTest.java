package io.tapdata.connector.mysql;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MysqlExceptionCollectorTest {

    @Test
    void testCollectCdcConfigInvalid(){

        MysqlExceptionCollector mysqlExceptionCollector = new MysqlExceptionCollector();
        String solutionSuggestions = "please open mysql binlog config";
        Throwable cause = new Exception(" Binlog config is close");
        try {
            mysqlExceptionCollector.collectCdcConfigInvalid(solutionSuggestions, cause);
        }catch (Throwable e){
            Assertions.assertTrue(e.getMessage().contains(solutionSuggestions));
        }
    }
}
