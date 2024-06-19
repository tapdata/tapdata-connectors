package io.tapdata.connector.tidb.util;

import io.tapdata.connector.tidb.util.pojo.ChangeFeed;
import io.tapdata.entity.logger.Log;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HttpUtilTest {
    HttpUtil httpUtil;

    CloseableHttpClient httpClient;
    Log tapLogger;

    CloseableHttpResponse response;
    @BeforeEach
    void init() throws IOException {
        tapLogger = mock(Log.class);
        httpClient = mock(CloseableHttpClient.class);
        httpUtil = mock(HttpUtil.class);
        response = mock(CloseableHttpResponse.class);

        when(httpClient.execute(any(HttpPost.class))).thenReturn(response);
        when(httpClient.execute(any(HttpRequestBase.class))).thenReturn(response);
        doNothing().when(httpClient).close();
        ReflectionTestUtils.setField(httpUtil, "httpClient", httpClient);
        ReflectionTestUtils.setField(httpUtil, "tapLogger", tapLogger);
    }

    @Test
    void testConfig() {
        HttpRequestBase base = mock(HttpRequestBase.class);
        when(httpUtil.config(base)).thenCallRealMethod();
        HttpRequestBase b = httpUtil.config(base);
        Assertions.assertNotNull(b);
        Assertions.assertEquals(base, b);
    }

    @Test
    void testCheckAlive() throws IOException {
        HttpGet httpGet = mock(HttpGet.class);
        when(httpUtil.config(any(HttpGet.class))).thenReturn(httpGet);
        when(httpUtil.checkAlive(anyString())).thenCallRealMethod();
        when(httpClient.execute(any(HttpGet.class))).thenAnswer(a -> {
            throw new IOException("");
        });
        Assertions.assertFalse(httpUtil.checkAlive("127.0.0.1:8300"));
    }

    @Test
    void testQueryChangeFeedsList() throws IOException {
        HttpGet httpGet = mock(HttpGet.class);
        when(httpUtil.config(any(HttpGet.class))).thenReturn(httpGet);
        when(httpUtil.queryChangeFeedsList(anyString())).thenCallRealMethod();
        when(httpClient.execute(any(HttpGet.class))).thenAnswer(a -> {
            throw new IOException("");
        });
        Assertions.assertThrows(IOException.class, () -> httpUtil.queryChangeFeedsList("127.0.0.1:8300"));
    }

    @Test
    void testDeleteChangeFeed() throws IOException {
        HttpDelete httpGet = mock(HttpDelete.class);
        when(httpUtil.config(any(HttpDelete.class))).thenReturn(httpGet);
        when(httpUtil.deleteChangeFeed(anyString(), anyString())).thenCallRealMethod();
        when(httpClient.execute(any(HttpDelete.class))).thenAnswer(a -> {
            throw new IOException("");
        });
        Assertions.assertThrows(IOException.class, () -> httpUtil.deleteChangeFeed("id", "127.0.0.1:8300"));
    }

    @Test
    void testCreateChangeFeed() throws IOException {
        HttpPost httpPost = mock(HttpPost.class);
        when(httpUtil.config(any(HttpPost.class))).thenReturn(httpPost);
        when(httpUtil.createChangeFeed(any(ChangeFeed.class), anyString())).thenCallRealMethod();
        when(httpClient.execute(any(HttpPost.class))).thenAnswer(a -> {
            throw new IOException("");
        });
        Assertions.assertThrows(IOException.class, () -> httpUtil.createChangeFeed(new ChangeFeed(), "127.0.0.1:8300"));
    }
}