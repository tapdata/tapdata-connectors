package io.tapdata.pdk.cli.services;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.tapdata.tm.sdk.auth.BasicCredentials;
import com.tapdata.tm.sdk.auth.Signer;
import com.tapdata.tm.sdk.util.Base64Util;
import com.tapdata.tm.sdk.util.IOUtil;
import com.tapdata.tm.sdk.util.SignUtil;
import io.tapdata.pdk.cli.services.request.FileProcess;
import io.tapdata.pdk.cli.services.request.InputStreamProcess;
import io.tapdata.pdk.cli.services.request.ProcessGroupInfo;
import io.tapdata.pdk.cli.utils.OkHttpUtils;
import io.tapdata.pdk.cli.utils.PrintUtil;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

public class UploadServiceWithProcess implements Uploader {
    boolean latest;
    String hostAndPort;
    String accessCode;
    String ak;
    String sk;
    PrintUtil printUtil;
    String token;
    boolean cloud;

    public UploadServiceWithProcess(PrintUtil printUtil, String hp, String ak, String sk, String accessCode, boolean latest) {
        this.printUtil = printUtil;
        this.latest = latest;
        this.hostAndPort = hp;
        this.ak = ak;
        this.sk = sk;
        this.accessCode = accessCode;
        cloud = StringUtils.isNotBlank(ak);
        try {
            if (!hostAndPort.startsWith("http")) {
                hostAndPort = "http://" + hostAndPort;
            }
            URL url = new URI(hostAndPort).toURL();
            String protocol = url.getProtocol();
            int port = url.getPort();
            if (port <= 0) {
                port = url.getDefaultPort();
            }
            this.hostAndPort = String.format("%s://%s:%d", StringUtils.isNotBlank(protocol) ? protocol : "http", url.getHost(), port);
        } catch (Exception e) {
            throw new RuntimeException("Service IP and port invalid: " + hostAndPort);
        }

        if (!cloud) {
            String tokenUrl = hostAndPort + "/api/users/generatetoken";
            Map<String, String> param = new HashMap<>();
            param.put("accesscode", accessCode);
            String jsonString = JSON.toJSONString(param);
            String s = OkHttpUtils.postJsonParams(tokenUrl, jsonString);

            printUtil.print(PrintUtil.TYPE.DEBUG, "generate token " + s);

            if (StringUtils.isNotBlank(s)) {
                Map<String, Object> map = JSON.parseObject(s, Map.class);
                Object data = map.get("data");
                if (null != data) {
                    JSONObject data1 = (JSONObject) data;
                    token = (String) data1.get("id");
                    if (StringUtils.isBlank(token)) {
                        token = null;
                    }
                }
            }
            if (null == token) {
                throw new RuntimeException("TM sever not found or generate token failed: " + hostAndPort);
            }
        }
    }

    public void upload(Map<String, InputStream> inputStreamMap, File file, List<String> jsons, String connectionType) {
        assert file != null;
        Map<String, BufferedInputStream> map = new HashMap<>();
        if (null != inputStreamMap && !inputStreamMap.isEmpty()) {
            inputStreamMap.forEach((name, stream) -> {
                map.put(name, new BufferedInputStream(stream, 1024 * 1024 * 10));
            });
        }
        if (cloud) {
            uploadToCloud(map, file, jsons, connectionType);
        } else {
            uploadToOp(map, file, jsons, connectionType);
        }
    }

    protected void uploadToCloud(Map<String, BufferedInputStream> inputStreamMap, File file, List<String> jsons, String connectionType) {
        Map<String, String> params = new HashMap<>();
        params.put("ts", String.valueOf(System.currentTimeMillis()));
        params.put("nonce", UUID.randomUUID().toString());
        params.put("signVersion", "1.0");
        params.put("accessKey", ak);
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        MultipartBody.Builder builder = new MultipartBody.Builder();
        builder.setType(MultipartBody.FORM);
        if (file != null) {
            digest.update("file".getBytes(UTF_8));
            digest.update(file.getName().getBytes(UTF_8));
            try {
                digest.update(IOUtil.readFile(file));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        if (inputStreamMap != null) {
            for (Map.Entry<String, BufferedInputStream> entry : inputStreamMap.entrySet()) {
                String k = entry.getKey();
                BufferedInputStream v = entry.getValue();
                byte[] in_b = null;
                try {
                    v.mark(Integer.MAX_VALUE);
                    ByteArrayOutputStream output = new ByteArrayOutputStream();
                    byte[] buf = new byte[1024];
                    int length = v.read(buf);
                    while (length > 0) {
                        output.write(buf, 0, length);
                        length = v.read(buf);
                    }
                    v.reset();
                    in_b = output.toByteArray();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                digest.update("file".getBytes(UTF_8));
                digest.update(k.getBytes(UTF_8));
                digest.update(in_b);
                inputStreamMap.put(k, v);
            }
        }

        //要上传的文字参数
        if (jsons != null && !jsons.isEmpty()) {
            for (String json : jsons) {
                digest.update("source".getBytes(UTF_8));
                digest.update(json.getBytes());
            }
            // if the jsons size == 1, the data received by TM will be weird, adding an empty string helps TM receive the
            // proper data; the empty string should be dealt in TM.
            if (jsons.size() == 1) {
                digest.update("source".getBytes(UTF_8));
                digest.update("".getBytes());
            }
        }    // whether replace the latest version
        String latestString = String.valueOf(latest);
        digest.update("latest".getBytes(UTF_8));
        digest.update(latestString.getBytes(UTF_8));


        String url;
        final String method = "POST";
        String bodyHash = Base64Util.encode(digest.digest());

        printUtil.print(PrintUtil.TYPE.DEBUG, String.format("bodyHash: %s", bodyHash));
        BasicCredentials basicCredentials = new BasicCredentials(ak, sk);
        Signer signer = Signer.getSigner(basicCredentials);


        String canonicalQueryString = SignUtil.canonicalQueryString(params);
        String stringToSign = String.format("%s:%s:%s", method, canonicalQueryString, bodyHash);
        printUtil.print(PrintUtil.TYPE.DEBUG, String.format("stringToSign: %s", stringToSign));
        String sign = signer.signString(stringToSign, basicCredentials);

        params.put("sign", sign);
        printUtil.print(PrintUtil.TYPE.DEBUG, "sign: " + sign);

        String queryString = params.keySet().stream().map(key -> {
            try {
                return String.format("%s=%s", SignUtil.percentEncode(key), SignUtil.percentEncode(params.get(key)));
            } catch (UnsupportedEncodingException e) {
                printUtil.print(PrintUtil.TYPE.WARN, e.getMessage());
                printUtil.print(PrintUtil.TYPE.DEBUG, Arrays.toString(e.getStackTrace()));
            }
            return key + "=" + params.get(key);
        }).collect(Collectors.joining("&"));
        url = hostAndPort + "/api/pdk/upload/source?" + queryString;

        upload(url, file, inputStreamMap, jsons, connectionType);
    }

    protected void uploadToOp(Map<String, BufferedInputStream> inputStreamMap, File file, List<String> jsons, String connectionType) {
        String url = hostAndPort + "/api/pdk/upload/source?access_token=" + token;
        upload(url, file, inputStreamMap, jsons, connectionType);
    }

    protected void upload(String url, File file, Map<String, BufferedInputStream> inputStreamMap, List<String> jsons, String connectionType) {
        String fileName = file.getName();
        OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(3000, TimeUnit.SECONDS)
                .readTimeout(3000, TimeUnit.SECONDS)
                .writeTimeout(3000, TimeUnit.SECONDS)
                .build();
        final AtomicBoolean lock = new AtomicBoolean(false);
        ProcessGroupInfo groupInfo = new ProcessGroupInfo(lock);
        MultipartBody.Builder builder = new MultipartBody.Builder();
        builder.setType(MultipartBody.FORM);
        RequestBody jarFileBody = new FileProcess(file, "application/java-archive", printUtil, groupInfo);
        groupInfo.addTotalBytes(file.length());
        builder.addFormDataPart("file", fileName, jarFileBody);

        if (inputStreamMap != null) {
            for (Map.Entry<String, BufferedInputStream> entry : inputStreamMap.entrySet()) {
                String entryName = entry.getKey();
                InputStreamProcess fileBody = new InputStreamProcess(entry.getValue(), "image/*", entryName, printUtil, groupInfo);
                groupInfo.addTotalBytes(fileBody.contentLength());
                builder.addFormDataPart("file", entryName, fileBody);
            }
        }

        //要上传的文字参数
        if (jsons != null && !jsons.isEmpty()) {
            for (String json : jsons) {
                builder.addPart(MultipartBody.Part.createFormData("source", json));
            }
            // if the jsons size == 1, the data received by TM will be weird, adding an empty string helps TM receive the
            // proper data; the empty string should be dealt in TM.
            if (jsons.size() == 1) {
                builder.addPart(MultipartBody.Part.createFormData("source", ""));
            }
        }
        // whether replace the latest version
        builder.addPart(MultipartBody.Part.createFormData("latest", String.valueOf(latest)));

        MultipartBody requestBody = builder.build();

        Request request = new Request.Builder()
                .url(url)
                .post(requestBody)
                .build();

        try (Response response = client.newCall(request).execute()) {
            groupInfo.getLock().compareAndSet(false, true);
            if (!response.isSuccessful()) {
                printUtil.print(PrintUtil.TYPE.ERROR, String.format("* Register Connector: %s | (%s) Completed", fileName, connectionType));
                return;
            }
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
