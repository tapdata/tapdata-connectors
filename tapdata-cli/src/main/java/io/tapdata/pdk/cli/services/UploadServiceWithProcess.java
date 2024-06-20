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
import io.tapdata.pdk.cli.utils.HttpRequest;
import io.tapdata.pdk.cli.utils.OkHttpUtils;
import io.tapdata.pdk.cli.utils.PrintUtil;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

public class UploadServiceWithProcess {
    boolean latest;
    String hostAndPort;
    String accessCode;
    String ak;
    String sk;
    PrintUtil printUtil;

    public UploadServiceWithProcess(PrintUtil printUtil, String hostAndPort, String ak, String sk, String accessCode, boolean latest) {
        this.printUtil = printUtil;
        this.latest = latest;
        this.hostAndPort = hostAndPort;
        this.ak = ak;
        this.sk = sk;
        this.accessCode = accessCode;
    }

    public void upload(Map<String, InputStream> inputStreamMap, File file, List<String> jsons) {
        boolean cloud = StringUtils.isNotBlank(ak);

        if (cloud) {
            uploadToCloud(inputStreamMap, file, jsons);
        } else {
            uploadToOp(inputStreamMap, file, jsons);
        }
    }

    protected void uploadToCloud(Map<String, InputStream> inputStreamMap, File file, List<String> jsons) {
        Map<String, String> params = new HashMap<>();
        params.put("ts", String.valueOf(System.currentTimeMillis()));
        params.put("nonce", UUID.randomUUID().toString());
        params.put("signVersion", "1.0");
        params.put("accessKey", ak);

        MessageDigest digest = null;
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
            for (Map.Entry<String, InputStream> entry : inputStreamMap.entrySet()) {
                String k = entry.getKey();
                InputStream v = entry.getValue();
                byte[] in_b = new byte[0];
                try {
                    in_b = IOUtil.readInputStream(v);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                v = new ByteArrayInputStream(in_b);
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
                e.printStackTrace();
            }
            return key + "=" + params.get(key);
        }).collect(Collectors.joining("&"));
        url = hostAndPort + "/api/pdk/upload/source?" + queryString;

        upload(url, file, inputStreamMap, jsons);
    }

    protected void uploadToOp(Map<String, InputStream> inputStreamMap, File file, List<String> jsons) {
        String tokenUrl = hostAndPort + "/api/users/generatetoken";
        Map<String, String> param = new HashMap<>();
        param.put("accesscode", accessCode);
        String jsonString = JSON.toJSONString(param);
        String s = OkHttpUtils.postJsonParams(tokenUrl, jsonString);

        printUtil.print(PrintUtil.TYPE.DEBUG, "generate token " + s);

        if (StringUtils.isBlank(s)) {
            printUtil.print(PrintUtil.TYPE.ERROR, "TM sever not found or generate token failed");
            return;
        }

        Map map = JSON.parseObject(s, Map.class);
        Object data = map.get("data");
        if (null == data) {
            printUtil.print(PrintUtil.TYPE.ERROR, "TM sever not found or generate token failed");
            return;
        }
        JSONObject data1 = (JSONObject) data;
        String token = (String) data1.get("id");
        if (StringUtils.isBlank(token)) {
            printUtil.print(PrintUtil.TYPE.ERROR, "TM sever not found or generate token failed");
            return;
        }
        String url = hostAndPort + "/api/pdk/upload/source?access_token=" + token;
        upload(url, file, inputStreamMap, jsons);
    }

    protected void upload(String url, File file, Map<String, InputStream> inputStreamMap, List<String> jsons) {
        OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(3000, TimeUnit.SECONDS)
                .readTimeout(3000, TimeUnit.SECONDS)
                .writeTimeout(3000, TimeUnit.SECONDS)
                .build();

        MultipartBody.Builder builder = new MultipartBody.Builder();
        builder.setType(MultipartBody.FORM);
        if (file != null) {
            RequestBody fileBody = new FileProcess(file, "application/java-archive", printUtil);
            builder.addFormDataPart("file", file.getName(), fileBody);
        }
        if (inputStreamMap != null) {
            for (Map.Entry<String, InputStream> entry : inputStreamMap.entrySet()) {
                String entryName = entry.getKey();
                InputStreamProcess fileBody = new InputStreamProcess(entry.getValue(), "image/*", entryName, printUtil);
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
            if (!response.isSuccessful()) {
                printUtil.print(PrintUtil.TYPE.ERROR, "Upload failed: " + response);
                return;
            }
            result(response.body().string(), file.getName());
        } catch (IOException e) {
            printUtil.print(PrintUtil.TYPE.WARN, e.getMessage());
            printUtil.print(PrintUtil.TYPE.DEBUG, Arrays.toString(e.getStackTrace()));
        }
    }

    protected void result(String response, String name) {
        Map<String, Object> responseMap = JSON.parseObject(response, Map.class);
        String msg = "success";
        String result = "success";
        if (!"ok".equals(responseMap.get("code"))) {
            msg = responseMap.get("reqId") != null ? (String) responseMap.get("message") : (String) responseMap.get("msg");
            result = "fail";
        }
        printUtil.print(PrintUtil.TYPE.DEBUG, "result:" + result + ", name:" + name + ", msg:" + msg + ", response:" + response);
    }
}
