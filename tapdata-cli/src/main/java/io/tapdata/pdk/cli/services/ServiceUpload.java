package io.tapdata.pdk.cli.services;

import com.alibaba.fastjson.JSON;
import io.tapdata.pdk.cli.services.request.ProcessGroupInfo;
import io.tapdata.pdk.cli.utils.PrintUtil;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface ServiceUpload {

    void upload(Map<String, BufferedInputStream> inputStreamMap, File file, List<String> jsons, String connectionType);


    default String fixUrl(String hostAndPort) {
        try {
            if (!hostAndPort.startsWith("http")) {
                hostAndPort = "http://" + hostAndPort;
            }
            URL url = new URI(hostAndPort).toURL();
            String protocol = url.getProtocol();
            if (StringUtils.isBlank(protocol)) {
                protocol = "http";
            }
            int port = url.getPort();
            String path = Optional.ofNullable(url.getPath()).orElse("");
            if (port <= 0) {
                hostAndPort = String.format("%s://%s%s", protocol, url.getHost(), path);
            } else {
                hostAndPort = String.format("%s://%s:%d%s", protocol, url.getHost(), port, path);
            }
            if (hostAndPort.endsWith("/")) {
                hostAndPort = hostAndPort.substring(0, hostAndPort.length() - 1);
            }
            return hostAndPort;
        } catch (Exception e) {
            throw new RuntimeException("Service IP and port invalid: " + hostAndPort);
        }
    }

    default void analyseResult(OkHttpClient client, Request request, ProcessGroupInfo groupInfo, PrintUtil printUtil, String fileName, String connectionType) {
        try (Response response = client.newCall(request).execute()) {
            groupInfo.getLock().compareAndSet(false, true);
//            if (!response.isSuccessful()) {
//                printUtil.print(PrintUtil.TYPE.ERROR, String.format("* Register Connector: %s | (%s) Failed", fileName, connectionType));
//                return;
//            }
            ResponseBody body = response.body();
            String msg = "{}";
            if (null != body) {
                msg = body.string();
            }
            Map<String, Object> responseMap = JSON.parseObject(msg, Map.class);
            if (!"ok".equalsIgnoreCase(String.valueOf(responseMap.get("code")))) {
                Object resultMsg = String.valueOf(Optional.ofNullable(Optional.ofNullable(responseMap.get("message")).orElse(responseMap.get("msg"))).orElse(msg));
                printUtil.print(PrintUtil.TYPE.ERROR, String.format("* Register Connector: %s | (%s) Failed, message: %s", fileName, connectionType, resultMsg));
            } else {
                printUtil.print(PrintUtil.TYPE.INFO, String.format("* Register Connector: %s | (%s) Completed", fileName, connectionType));
            }
        } catch (IOException e) {
            groupInfo.getLock().compareAndSet(false, true);
            printUtil.print(PrintUtil.TYPE.ERROR, String.format("* Register Connector: %s | (%s) Failed, message: %s", fileName, connectionType, e.getMessage()));
            printUtil.print(PrintUtil.TYPE.DEBUG, Arrays.toString(e.getStackTrace()));
        }
    }
}
