package io.tapdata.connector.doris.streamload;

import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HttpUtilTest {
    @Test
    void test1(){
        Assertions.assertDoesNotThrow(HttpUtil::generationHttpClient);
    }
}
