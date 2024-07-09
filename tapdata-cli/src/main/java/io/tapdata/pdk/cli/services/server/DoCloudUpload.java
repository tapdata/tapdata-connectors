package io.tapdata.pdk.cli.services.server;

import com.tapdata.tm.sdk.auth.BasicCredentials;
import com.tapdata.tm.sdk.auth.Signer;
import com.tapdata.tm.sdk.util.Base64Util;
import com.tapdata.tm.sdk.util.IOUtil;
import com.tapdata.tm.sdk.util.SignUtil;
import io.tapdata.pdk.cli.services.MyCloudMultipartBody;
import io.tapdata.pdk.cli.services.ServiceUpload;
import io.tapdata.pdk.cli.services.request.ByteArrayCollector;
import io.tapdata.pdk.cli.services.request.ByteArrayProcess;
import io.tapdata.pdk.cli.services.request.ProcessGroupInfo;
import io.tapdata.pdk.cli.services.request.ProgressRequestBody;
import io.tapdata.pdk.cli.utils.PrintUtil;
import okhttp3.Credentials;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

public class DoCloudUpload implements ServiceUpload {
    public static final String URL = "%s/api/pdk/upload/source?%s";

    String hostAndPort;
    String ak;
    String sk;
    PrintUtil printUtil;
    String latestString;


    public DoCloudUpload(PrintUtil printUtil, String hp, String ak, String sk, boolean latest) {
        this.printUtil = printUtil;
        this.ak = ak;
        this.sk = sk;
        this.hostAndPort = fixUrl(hp);
        this.latestString = String.valueOf(latest);
    }

    public void upload(Map<String, BufferedInputStream> inputStreamMap, File file, List<String> jsons, String connectionType) {
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
        digest.update("file".getBytes(UTF_8));
        digest.update(file.getName().getBytes(UTF_8));
        try {
            digest.update(IOUtil.readFile(file));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (inputStreamMap != null) {
            for (Map.Entry<String, BufferedInputStream> entry : inputStreamMap.entrySet()) {
                String k = entry.getKey();
                InputStream v = entry.getValue();
                byte[] in_b = new byte[0];
                try {
                    v.mark(Integer.MAX_VALUE);
                    in_b = IOUtil.readInputStream(v, false);
                    v.reset();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                v = new ByteArrayInputStream(in_b);
                digest.update("file".getBytes(UTF_8));
                digest.update(k.getBytes(UTF_8));
                digest.update(in_b);
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
        }
        // whether replace the latest version
        digest.update("latest".getBytes(UTF_8));
        digest.update(latestString.getBytes(UTF_8));

        String url;
        final String method = "POST";
        ByteArrayCollector request;
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
        request = new ByteArrayCollector();
        request.part("file", file.getName(), "application/java-archive", file);
        if (inputStreamMap != null) {
            for (Map.Entry<String, BufferedInputStream> entry : inputStreamMap.entrySet()) {
                String k = entry.getKey();
                request.part("file", k, "image/*", entry.getValue());
            }
        }

        //要上传的文字参数
        if (jsons != null && !jsons.isEmpty()) {
            for (String json : jsons) {
                request.part("source", json);
            }
            // if the jsons size == 1, the data received by TM will be weird, adding an empty string helps TM receive the
            // proper data; the empty string should be dealt in TM.
            if (jsons.size() == 1) {
                request.part("source", "");
            }
        }
        // whether replace the latest version
        request.part("latest", latestString);

        AtomicBoolean lock = new AtomicBoolean(false);
        ByteArrayCollector.RequestOutputStream output = request.getOutput();

        byte[] bytes;
        try {
            try {
                output.write("\r\n--" + ByteArrayCollector.BOUNDARY + "--\r\n");
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            bytes = output.toByteArray();
        } finally {
            if (null != output) {
                try {
                    output.close();
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                }
            }
        }
        List<ProgressRequestBody<?>> body = new ArrayList<>();
        ProcessGroupInfo groupInfo = new ProcessGroupInfo(lock);
        ProgressRequestBody<byte[]> fileBody = new ByteArrayProcess(null, bytes, MultipartBody.FORM.type(), printUtil, groupInfo);
        body.add(fileBody);
        groupInfo.addTotalBytes(fileBody.length());
        upload(url, file.getName(), groupInfo, body, connectionType, sign);
    }

    protected void upload(String url, String fileName, ProcessGroupInfo groupInfo, List<ProgressRequestBody<?>> body, String connectionType, String signature) {
        OkHttpClient client = new OkHttpClient.Builder()
                .protocols(Arrays.asList(Protocol.HTTP_1_1))
                .connectTimeout(300, TimeUnit.SECONDS)
                .readTimeout(300, TimeUnit.SECONDS)
                .writeTimeout(300, TimeUnit.SECONDS)
                .build();

        MyCloudMultipartBody.Builder builder = new MyCloudMultipartBody.Builder("00content0boundary00");
        builder.setType(MultipartBody.FORM);

        for (ProgressRequestBody<?> requestBody : body) {
            builder.addFormDataPart(requestBody.name(), requestBody.fileName(), requestBody);
        }

        MyCloudMultipartBody requestBody = builder.build();
        Request request = new Request.Builder()
                .url(url)
                .addHeader("Authorization", "Basic " + signature)
                .addHeader("Expect", "100-continue")
                .addHeader("Signature", signature)
                .post(requestBody)
                .build();

        analyseResult(client, request, groupInfo, printUtil, fileName, connectionType);
    }
}
