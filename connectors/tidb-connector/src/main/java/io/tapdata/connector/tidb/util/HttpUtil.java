package io.tapdata.connector.tidb.util;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import io.tapdata.connector.tidb.util.pojo.ChangeFeed;
import io.tapdata.entity.logger.Log;
import io.tapdata.kit.ErrorKit;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class HttpUtil implements AutoCloseable {

    private final CloseableHttpClient httpClient;
    private boolean isChangeFeedClosed;

    protected Log tapLogger;

    public HttpUtil(Log tapLogger) {
        httpClient = HttpClientBuilder.create().build();
        this.tapLogger = tapLogger;
    }

    public Boolean deleteChangefeed(String changefeedId, String cdcUrl) throws IOException {
        // cdcUrl = "192.168.1.179:8300";
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
                tapLogger.info("delete Changefeed is success changefeedId:{}",changefeedId);
                return true;
            }else {
                tapLogger.error("delete Changefeed is fail errMsg:{}",EntityUtils.toString(response.getEntity()));
            }
        }
        return false;
    }

    public Boolean createChangefeed(ChangeFeed changefeed, String cdcUrl) throws IOException {
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
                tapLogger.info("Create Changefeed is success changefeedId:{}",changefeed.getChangefeedId());
                return true;
            }else {
                String toString = EntityUtils.toString(responseEntity);
                tapLogger.info("Create Changefeed is fail errorMsg:{}", toString);
                throw new IOException("Tidb stream fail reasoon:" + toString);
            }
        }
    }
public  Boolean resumeChangefeed(String changefeedId,String cdcUrl)throws IOException{
    String url ="http://"+ cdcUrl+"/api/v2/changefeeds/"+changefeedId+"/resume";
    HttpPost httpPost = new HttpPost(url);
    httpPost.setHeader("Content-Type", "application/json;charset=utf8");
    try (
            CloseableHttpResponse response = httpClient.execute(httpPost)
    ) {
        if (response.getStatusLine().getStatusCode() == 200) {
            isChangeFeedClosed = false;
            return true;
        }
    }
    return false;
}
    public  Boolean pauseChangefeed(String changefeedId,String cdcUrl)throws IOException{
        String url ="http://"+ cdcUrl+"/api/v2/changefeeds/"+changefeedId+"/pause";
        HttpPost httpPost = new HttpPost(url);
        httpPost.setHeader("Content-Type", "application/json;charset=utf8");
        try (
                CloseableHttpResponse response = httpClient.execute(httpPost)
        ) {
            if (response.getStatusLine().getStatusCode() == 202) {
                isChangeFeedClosed = false;
                return true;
            }
        }
        return false;
    }

    public void close() {
        ErrorKit.ignoreAnyError(httpClient::close);
    }
    public boolean isChangeFeedClosed() {
        return isChangeFeedClosed;
    }
}


