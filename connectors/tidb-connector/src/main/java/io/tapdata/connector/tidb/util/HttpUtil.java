package io.tapdata.connector.tidb.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import io.tapdata.connector.tidb.util.pojo.ChangeFeed;
import io.tapdata.entity.logger.Log;
import io.tapdata.kit.ErrorKit;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class HttpUtil implements AutoCloseable {

    private final CloseableHttpClient httpClient;

    protected Log tapLogger;

    public HttpUtil(Log tapLogger) {
        httpClient = HttpClientBuilder.create().build();
        this.tapLogger = tapLogger;
    }

    public Boolean deleteChangeFeed(String changefeedId, String cdcUrl) throws IOException {
        String url = "http://" + cdcUrl + "/api/v2/changefeeds/" + changefeedId;
        HttpDelete httpDelete = new HttpDelete(url);
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(200000)
                .setConnectionRequestTimeout(200000)
                .setSocketTimeout(200000)
                .setRedirectsEnabled(true).build();
        httpDelete.setConfig(requestConfig);
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

    public Boolean createChangeFeed(ChangeFeed changefeed, String cdcUrl) throws IOException {
        String url = "http://" + cdcUrl + "/api/v2/changefeeds";
        HttpPost httpPost = new HttpPost(url);
        SerializeConfig config = new SerializeConfig();
        config.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;
        httpPost.setEntity(new StringEntity(JSON.toJSONString(changefeed, config), "UTF-8"));
        httpPost.setHeader("Content-Type", "application/json;charset=utf8");
        try (
                CloseableHttpResponse response = httpClient.execute(httpPost)
        ) {
            HttpEntity responseEntity = response.getEntity();
            if (responseEntity != null && response.getStatusLine().getStatusCode() == 200) {
                tapLogger.info("Create change feed succeed, change feed id: {}", changefeed.getChangefeedId());
                return true;
            } else {
                throw new IOException("TiDB stream failed: " + (null == responseEntity ? "" : EntityUtils.toString(responseEntity)));
            }
        }
    }


    public int queryChangeFeedsList(String cdcUrl) throws IOException {
        String url = "http://" + cdcUrl + "/api/v2/changefeeds";
        HttpGet httpGet = new HttpGet(url);
        try (
                CloseableHttpResponse response = httpClient.execute(httpGet)
        ) {
            HttpEntity responseEntity = response.getEntity();
            if (responseEntity != null && response.getStatusLine().getStatusCode() == 200) {
                String toString = EntityUtils.toString(responseEntity);
                JSONObject jsonObject = JSON.parseObject(toString);
                int taskNum = (int) jsonObject.get("total");
                tapLogger.debug("Query change feeds list succeed, task count: {}", taskNum);
                return taskNum;
            } else {
                throw new IOException("Query change feeds list failed, message: " + (null == responseEntity ? "" : EntityUtils.toString(responseEntity)));
            }
        }
    }

    public boolean checkAlive(String serverUrl) {
        String url = "http://" + serverUrl + "/api/v2/health";
        HttpGet httpGet = new HttpGet(url);
        try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
            HttpEntity responseEntity = response.getEntity();
            return responseEntity != null && response.getStatusLine().getStatusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    public void close() {
        ErrorKit.ignoreAnyError(httpClient::close);
    }
}


