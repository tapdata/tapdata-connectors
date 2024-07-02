package io.tapdata.connector.tidb.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.tapdata.connector.tidb.util.pojo.ChangeFeed;
import io.tapdata.entity.logger.Log;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
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
        when(httpUtil.queryChangeFeeds(anyString())).thenCallRealMethod();
        when(httpClient.execute(any(HttpGet.class))).thenAnswer(a -> {
            throw new IOException("");
        });
        doNothing().when(tapLogger).warn(anyString(), anyString());
        Assertions.assertDoesNotThrow(() -> httpUtil.queryChangeFeedsList("127.0.0.1:8300"));
    }

    @Test
    void testQueryChangeFeeds() throws IOException {
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        HttpEntity responseEntity = mock(HttpEntity.class);
        StatusLine statusLine = mock(StatusLine.class);
        HttpGet httpGet = mock(HttpGet.class);
        when(httpUtil.config(any(HttpGet.class))).thenReturn(httpGet);
        when(httpUtil.queryChangeFeeds(anyString())).thenCallRealMethod();
        when(httpClient.execute(any(HttpGet.class))).thenReturn(response);
        when(response.getEntity()).thenReturn(responseEntity);
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        doNothing().when(tapLogger).warn(anyString(), anyString());
        JSONObject jsonObject = new JSONObject();
        try(MockedStatic<EntityUtils> eu = mockStatic(EntityUtils.class);
            MockedStatic<JSON> jp = mockStatic(JSON.class)) {
            jp.when(() -> JSON.parseObject(anyString())).thenReturn(jsonObject);
            eu.when(() -> EntityUtils.toString(responseEntity)).thenReturn("{}");
            Assertions.assertDoesNotThrow(() -> httpUtil.queryChangeFeeds("127.0.0.1:8300"));
        }
    }
    @Test
    void testQueryChangeFeeds1() throws IOException {
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        HttpEntity responseEntity = mock(HttpEntity.class);
        StatusLine statusLine = mock(StatusLine.class);
        HttpGet httpGet = mock(HttpGet.class);
        when(httpUtil.config(any(HttpGet.class))).thenReturn(httpGet);
        when(httpUtil.queryChangeFeeds(anyString())).thenCallRealMethod();
        when(httpClient.execute(any(HttpGet.class))).thenReturn(response);
        when(response.getEntity()).thenReturn(responseEntity);
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        doNothing().when(tapLogger).warn(anyString(), anyString());
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("items", new ArrayList<>());
        try(MockedStatic<EntityUtils> eu = mockStatic(EntityUtils.class);
            MockedStatic<JSON> jp = mockStatic(JSON.class)) {
            eu.when(() -> EntityUtils.toString(responseEntity)).thenReturn("{\"item\":[]}");
            jp.when(() -> JSON.parseObject(anyString())).thenReturn(jsonObject);
            Assertions.assertDoesNotThrow(() -> httpUtil.queryChangeFeeds("127.0.0.1:8300"));
        }
    }
    @Test
    void testQueryChangeFeeds3() throws IOException {
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        HttpEntity responseEntity = mock(HttpEntity.class);
        StatusLine statusLine = mock(StatusLine.class);
        HttpGet httpGet = mock(HttpGet.class);
        when(httpUtil.config(any(HttpGet.class))).thenReturn(httpGet);
        when(httpUtil.queryChangeFeeds(anyString())).thenCallRealMethod();
        when(httpClient.execute(any(HttpGet.class))).thenReturn(response);
        when(response.getEntity()).thenReturn(responseEntity);
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        doNothing().when(tapLogger).warn(anyString(), anyString());
        JSONObject jsonObject = new JSONObject();
        ArrayList<Object> objects = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        map.put("id", "id");
        objects.add(map);
        jsonObject.put("items", objects);
        try(MockedStatic<EntityUtils> eu = mockStatic(EntityUtils.class);
            MockedStatic<JSON> jp = mockStatic(JSON.class)) {
            jp.when(() -> JSON.parseObject(anyString())).thenReturn(jsonObject);
            eu.when(() -> EntityUtils.toString(responseEntity)).thenReturn("{\"item\":[{\"id\":\"sss\"}]}");
            Assertions.assertDoesNotThrow(() -> httpUtil.queryChangeFeeds("127.0.0.1:8300"));
        }
    }
    @Test
    void testQueryChangeFeeds4() throws IOException {
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        HttpEntity responseEntity = mock(HttpEntity.class);
        StatusLine statusLine = mock(StatusLine.class);
        HttpGet httpGet = mock(HttpGet.class);
        when(httpUtil.config(any(HttpGet.class))).thenReturn(httpGet);
        when(httpUtil.queryChangeFeeds(anyString())).thenCallRealMethod();
        when(httpClient.execute(any(HttpGet.class))).thenReturn(response);
        when(response.getEntity()).thenReturn(null);
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        doNothing().when(tapLogger).warn(anyString(), anyString());
        JSONObject jsonObject = new JSONObject();
        ArrayList<Object> objects = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        map.put("id", "sss");
        objects.add(map);
        jsonObject.put("items", objects);
        try(MockedStatic<EntityUtils> eu = mockStatic(EntityUtils.class);
            MockedStatic<JSON> jp = mockStatic(JSON.class)) {
            jp.when(() -> JSON.parseObject(anyString())).thenReturn(jsonObject);
            eu.when(() -> EntityUtils.toString(responseEntity)).thenReturn("{\"item\":[{\"id\":\"sss\"}]}");
            Assertions.assertDoesNotThrow(() -> httpUtil.queryChangeFeeds("127.0.0.1:8300"));
        }
    }
    @Test
    void testQueryChangeFeeds5() throws IOException {
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        HttpEntity responseEntity = mock(HttpEntity.class);
        StatusLine statusLine = mock(StatusLine.class);
        HttpGet httpGet = mock(HttpGet.class);
        when(httpUtil.config(any(HttpGet.class))).thenReturn(httpGet);
        when(httpUtil.queryChangeFeeds(anyString())).thenCallRealMethod();
        when(httpClient.execute(any(HttpGet.class))).thenReturn(response);
        when(response.getEntity()).thenReturn(null);
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(400);
        doNothing().when(tapLogger).warn(anyString(), anyString());
        JSONObject jsonObject = new JSONObject();
        ArrayList<Object> objects = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        map.put("id", "id");
        objects.add(map);
        jsonObject.put("items", objects);
        try(MockedStatic<EntityUtils> eu = mockStatic(EntityUtils.class);
            MockedStatic<JSON> jp = mockStatic(JSON.class)) {
            jp.when(() -> JSON.parseObject(anyString())).thenReturn(jsonObject);
            eu.when(() -> EntityUtils.toString(responseEntity)).thenReturn("{\"item\":[{\"id\":\"sss\"}]}");
            Assertions.assertDoesNotThrow(() -> httpUtil.queryChangeFeeds("127.0.0.1:8300"));
        }
    }
    @Test
    void testQueryChangeFeeds6() throws IOException {
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        HttpEntity responseEntity = mock(HttpEntity.class);
        StatusLine statusLine = mock(StatusLine.class);
        HttpGet httpGet = mock(HttpGet.class);
        when(httpUtil.config(any(HttpGet.class))).thenReturn(httpGet);
        when(httpUtil.queryChangeFeeds(anyString())).thenCallRealMethod();
        when(httpClient.execute(any(HttpGet.class))).thenReturn(response);
        when(response.getEntity()).thenReturn(responseEntity);
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(400);
        doNothing().when(tapLogger).warn(anyString(), anyString());
        JSONObject jsonObject = new JSONObject();
        ArrayList<Object> objects = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        map.put("id", "id");
        objects.add(map);
        jsonObject.put("items", objects);
        try(MockedStatic<EntityUtils> eu = mockStatic(EntityUtils.class);
            MockedStatic<JSON> jp = mockStatic(JSON.class)) {
            jp.when(() -> JSON.parseObject(anyString())).thenReturn(jsonObject);
            eu.when(() -> EntityUtils.toString(responseEntity)).thenReturn("{\"item\":[{\"id\":\"sss\"}]}");
            Assertions.assertDoesNotThrow(() -> httpUtil.queryChangeFeeds("127.0.0.1:8300"));
        }
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
    @Test
    void testQueryChangeFeedsList1() throws IOException {
        when(httpUtil.queryChangeFeeds(anyString())).thenReturn(new ArrayList<>());
        when(httpUtil.queryChangeFeedsList(anyString(), anyString())).thenCallRealMethod();
        Assertions.assertFalse(httpUtil.queryChangeFeedsList("", ""));
    }
    @Test
    void testQueryChangeFeedsList11() throws IOException {
        when(httpUtil.queryChangeFeeds(anyString())).thenReturn(null);
        when(httpUtil.queryChangeFeedsList(anyString(), anyString())).thenCallRealMethod();
        Assertions.assertFalse(httpUtil.queryChangeFeedsList("", ""));
    }
    @Test
    void testQueryChangeFeedsList2() throws IOException {
        List<Map<String, Object>> items = new ArrayList<>();
        items.add(null);
        Map<String, Object> map = new HashMap<>();
        items.add(map);
        Map<String, Object> map1 = new HashMap<>();
        map1.put("id", "i");
        items.add(map1);
        when(httpUtil.queryChangeFeeds(anyString())).thenReturn(items);
        when(httpUtil.queryChangeFeedsList(anyString(), anyString())).thenCallRealMethod();
        Assertions.assertFalse(httpUtil.queryChangeFeedsList("", "id"));
    }
    @Test
    void testQueryChangeFeedsList21() throws IOException {
        List<Map<String, Object>> items = new ArrayList<>();
        items.add(null);
        Map<String, Object> map = new HashMap<>();
        items.add(map);
        Map<String, Object> map1 = new HashMap<>();
        map1.put("id", "i");
        items.add(map1);
        Map<String, Object> map2 = new HashMap<>();
        map1.put("id", "id");
        items.add(map2);
        when(httpUtil.queryChangeFeeds(anyString())).thenReturn(items);
        when(httpUtil.queryChangeFeedsList(anyString(), anyString())).thenCallRealMethod();
        Assertions.assertFalse(httpUtil.queryChangeFeedsList("", "id"));
    }
    @Test
    void testQueryChangeFeedsList22() throws IOException {
        List<Map<String, Object>> items = new ArrayList<>();
        items.add(null);
        Map<String, Object> map = new HashMap<>();
        items.add(map);
        Map<String, Object> map1 = new HashMap<>();
        map1.put("id", "i");
        items.add(map1);
        Map<String, Object> map2 = new HashMap<>();
        map1.put("id", "id");
        map1.put("state", "id");
        items.add(map2);
        when(httpUtil.queryChangeFeeds(anyString())).thenReturn(items);
        when(httpUtil.queryChangeFeedsList(anyString(), anyString())).thenCallRealMethod();
        Assertions.assertFalse(httpUtil.queryChangeFeedsList("", "id"));
    }
    @Test
    void testQueryChangeFeedsList23() throws IOException {
        List<Map<String, Object>> items = new ArrayList<>();
        items.add(null);
        Map<String, Object> map = new HashMap<>();
        items.add(map);
        Map<String, Object> map1 = new HashMap<>();
        map1.put("id", "i");
        items.add(map1);
        Map<String, Object> map2 = new HashMap<>();
        map1.put("id", "id");
        map1.put("state", "normal");
        items.add(map2);
        when(httpUtil.queryChangeFeeds(anyString())).thenReturn(items);
        when(httpUtil.queryChangeFeedsList(anyString(), anyString())).thenCallRealMethod();
        Assertions.assertTrue(httpUtil.queryChangeFeedsList("", "id"));
    }
}