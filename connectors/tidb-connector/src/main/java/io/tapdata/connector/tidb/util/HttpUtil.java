package io.tapdata.connector.tidb.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import io.tapdata.connector.tidb.util.pojo.ChangeFeed;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kit.ErrorKit;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class HttpUtil implements AutoCloseable {
    public static final int ERROR_START_TS_BEFORE_GC = 100012;
    private final CloseableHttpClient httpClient;

    protected Log tapLogger;

    public static HttpUtil of(Log tapLogger) {
        return new HttpUtil(tapLogger);
    }

    private HttpUtil(Log tapLogger) {
        httpClient = HttpClientBuilder.create().build();
        this.tapLogger = tapLogger;
    }

    public Boolean deleteChangeFeed(String changefeedId, String cdcUrl) throws IOException {
        String url = String.format("http://%s/api/v2/changefeeds/%s",cdcUrl, changefeedId);
        HttpDelete httpDelete = (HttpDelete) config(new HttpDelete(url));
        try (
                CloseableHttpResponse response = httpClient.execute(httpDelete)
        ) {
            if (response.getStatusLine().getStatusCode() == 200) {
                tapLogger.debug("Delete change feed succeed change feedId: {}", changefeedId);
                return true;
            } else {
                tapLogger.warn("Delete change feed failed error message: {}", EntityUtils.toString(response.getEntity()));
            }
        }
        return false;
    }

    public boolean changeFeedNotExists(String changeFeedId, String cdcUrl) {
        String url = String.format("http://%s/api/v2/changefeeds/%s",cdcUrl, changeFeedId);
        HttpGet httpDelete = (HttpGet) config(new HttpGet(url));
        try (
                CloseableHttpResponse response = httpClient.execute(httpDelete)
        ) {
            String msg = EntityUtils.toString(response.getEntity());
            if (response.getStatusLine().getStatusCode() == 400) {
                return "CDC:ErrChangeFeedNotExists".equalsIgnoreCase(String.valueOf(TapSimplify.fromJson(msg, Map.class).get("error_code")));
            } else {
                return false;
            }
        } catch (Exception e) {
            tapLogger.debug("Get change feed failed: {}, will retry again", e.getMessage());
            return false;
        }
    }

    public Boolean createChangeFeed(ChangeFeed changefeed, String cdcUrl) throws IOException {
        String url = String.format("http://%s/api/v2/changefeeds", cdcUrl);
        HttpPost httpPost = (HttpPost) config(new HttpPost(url));
        SerializeConfig config = new SerializeConfig();
        config.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;
        httpPost.setEntity(new StringEntity(JSON.toJSONString(changefeed, config), "UTF-8"));
        httpPost.setHeader("Content-Type", "application/json;charset=utf8");
        try (
                CloseableHttpResponse response = httpClient.execute(httpPost)
        ) {
            HttpEntity responseEntity = response.getEntity();
            if (responseEntity != null && response.getStatusLine().getStatusCode() == 200) {
                tapLogger.debug("Create change feed succeed, request body: {}", EntityUtils.toString(responseEntity));
                tapLogger.info("Create change feed succeed, change feed id: {}", changefeed.getChangefeedId());
                return true;
            } else {
                String msg = null == responseEntity ? "{}" : EntityUtils.toString(responseEntity);
                Map<String, String> result = TapSimplify.fromJson(msg, Map.class);
                if ("CDC:ErrStartTsBeforeGC".equalsIgnoreCase(result.get("error_code"))) {
                    throw new CoreException(ERROR_START_TS_BEFORE_GC, "TiDB stream failed: {}", msg);
                }
                throw new IOException("TiDB stream failed: " + msg);
            }
        }
    }


    public int queryChangeFeedsList(String cdcUrl) {
        return queryChangeFeeds(cdcUrl).size();
    }

    public boolean queryChangeFeedsList(String cdcUrl, String changeFeedId) {
        List<Map<String, Object>> items = queryChangeFeeds(cdcUrl);
        if (null == items || items.isEmpty()) return false;
        Optional<Map<String, Object>> first = items.stream().filter(Objects::nonNull)
                .filter(c -> Objects.nonNull(c.get("id")))
                .filter(c -> changeFeedId.equals(c.get("id")))
                .findFirst();
        if (first.isPresent()) {
            Map<String, Object> info = first.get();
            Object state = info.get("state");
            return Objects.nonNull(state) && "normal".equals(state);
        }
        return false;
    }

    public List<Map<String, Object>> queryChangeFeeds(String cdcUrl) {
        String url = String.format("http://%s/api/v2/changefeeds", cdcUrl);
        HttpGet httpGet = (HttpGet) config(new HttpGet(url));
        try (
                CloseableHttpResponse response = httpClient.execute(httpGet)
        ) {
            HttpEntity responseEntity = response.getEntity();
            if (responseEntity != null && response.getStatusLine().getStatusCode() == 200) {
                String toString = EntityUtils.toString(responseEntity);
                JSONObject jsonObject = JSON.parseObject(toString);
                List<Map<String, Object>> items = (List<Map<String, Object>>) jsonObject.get("items");
                if (null == items || items.isEmpty()) return new ArrayList<>();
                return items;
            } else {
                tapLogger.warn("Query change feeds list failed, message: {}", (null == responseEntity ? "" : EntityUtils.toString(responseEntity)));
            }
        } catch (Exception e) {
            tapLogger.warn("Query change feeds list with an exception, message: {}", e.getMessage());
        }
        return new ArrayList<>();
    }

    public boolean checkAlive(String serverUrl) {
        String url = String.format("http://%s/api/v2/health", serverUrl);
        HttpGet httpGet = (HttpGet) config(new HttpGet(url));
        try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
            HttpEntity responseEntity = response.getEntity();
            return responseEntity != null && response.getStatusLine().getStatusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    protected HttpRequestBase config(HttpRequestBase requestBase) {
        RequestConfig.Builder custom = RequestConfig.custom();
        custom.setConnectTimeout(10000);
        custom.setConnectTimeout(10000);
        custom.setConnectionRequestTimeout(10000);
        custom.setSocketTimeout(10000);
        custom.setRedirectsEnabled(true);
        RequestConfig build = custom.build();
        requestBase.setConfig(build);
        return requestBase;
    }

    public void close() {
        ErrorKit.ignoreAnyError(httpClient::close);
    }
}


