package io.tapdata.pdk.cli.services.server;

import com.tapdata.tm.sdk.auth.BasicCredentials;
import com.tapdata.tm.sdk.auth.Signer;
import com.tapdata.tm.sdk.util.Base64Util;
import com.tapdata.tm.sdk.util.IOUtil;
import com.tapdata.tm.sdk.util.SignUtil;
import io.tapdata.pdk.cli.services.MyCloudMultipartBody;
import io.tapdata.pdk.cli.services.ServiceUpload;
import io.tapdata.pdk.cli.services.request.FileProcess;
import io.tapdata.pdk.cli.services.request.InputStreamProcess;
import io.tapdata.pdk.cli.services.request.ProcessGroupInfo;
import io.tapdata.pdk.cli.services.request.ProgressRequestBody;
import io.tapdata.pdk.cli.services.request.StringProcess;
import io.tapdata.pdk.cli.utils.PrintUtil;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.RequestBody;
import okio.BufferedSink;
import okio.Okio;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
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

    boolean latest;
    String hostAndPort;
    String ak;
    String sk;
    PrintUtil printUtil;

    public DoCloudUpload(PrintUtil printUtil, String hp, String ak, String sk, boolean latest) {
        this.printUtil = printUtil;
        this.latest = latest;
        this.ak = ak;
        this.sk = sk;
        this.hostAndPort = fixUrl(hp);
    }

    @Override
    public void upload(Map<String, BufferedInputStream> inputStreamMap, File file, List<String> jsons, String connectionType) {
        final AtomicBoolean lock = new AtomicBoolean(false);
        ProcessGroupInfo groupInfo = new ProcessGroupInfo(lock);

        assert file != null;
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

        digest.update("file".getBytes(UTF_8));
        digest.update(file.getName().getBytes(UTF_8));
        try {
            byte[] bytes = IOUtil.readFile(file);
            digest.update(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (inputStreamMap != null) {
            for (Map.Entry<String, BufferedInputStream> entry : inputStreamMap.entrySet()) {
                String k = entry.getKey();
                BufferedInputStream v = entry.getValue();
                byte[] in_b = null;
                try {
                    v.mark(Integer.MAX_VALUE);
                    in_b = IOUtil.readInputStream(v, false);
                    v.reset();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                digest.update("file".getBytes(UTF_8));
                digest.update(k.getBytes(UTF_8));
                digest.update(in_b);
            }
        }

        //要上传的文字参数
        if (jsons != null && !jsons.isEmpty()) {
            for (String json : jsons) {
                digest.update("source".getBytes(UTF_8));
                digest.update(json.getBytes(UTF_8));
            }
            // if the jsons size == 1, the data received by TM will be weird, adding an empty string helps TM receive the
            // proper data; the empty string should be dealt in TM.
            if (jsons.size() == 1) {
                digest.update("source".getBytes(UTF_8));
                digest.update("".getBytes(UTF_8));
            }
        }    // whether replace the latest version
        String latestString = String.valueOf(latest);
        digest.update("latest".getBytes(UTF_8));
        digest.update(latestString.getBytes(UTF_8));

        List<ProgressRequestBody<?>> body = body(groupInfo, file, inputStreamMap, jsons);


        // Include content length
//        digest.update("Content-Length".getBytes(UTF_8));
//        long contentLength = groupInfo.totalByte;
//        digest.update(String.valueOf(contentLength).getBytes(UTF_8));

        // Finalize digest with secret key
        //digest.update(sk.getBytes(UTF_8));


        String url;
        final String method = "POST";
        String bodyHash = Base64Util.encode(digest.digest());

        printUtil.print(PrintUtil.TYPE.DEBUG, String.format("Body Hash: %s", bodyHash));
        BasicCredentials basicCredentials = new BasicCredentials(ak, sk);
        Signer signer = Signer.getSigner(basicCredentials);


        String canonicalQueryString = SignUtil.canonicalQueryString(params);
        String stringToSign = String.format("%s:%s:%s", method, canonicalQueryString, bodyHash);
        printUtil.print(PrintUtil.TYPE.DEBUG, String.format("Sign String: %s", stringToSign));
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
        url = String.format(URL, hostAndPort, queryString);

        uploadV2(url, file.getName(), groupInfo, body, connectionType, sign);
        //upload(url, file, inputStreamMap, jsons, connectionType, sign);
    }


    protected List<ProgressRequestBody<?>> body(ProcessGroupInfo groupInfo, File file, Map<String, BufferedInputStream> inputStreamMap, List<String> jsons) {
        List<ProgressRequestBody<?>> body = new ArrayList<>();
        try {
            ProgressRequestBody<File> jarFileBody = new FileProcess(file, "application/java-archive", printUtil, groupInfo);
            body.add(jarFileBody);
            groupInfo.addTotalBytes(jarFileBody.contentLength());
        } catch (Exception e) {

        }
        if (inputStreamMap != null) {
            for (Map.Entry<String, BufferedInputStream> entry : inputStreamMap.entrySet()) {
                String entryName = entry.getKey();
                ProgressRequestBody<BufferedInputStream> fileBody = new InputStreamProcess(entry.getValue(), "image/*", entryName, printUtil, groupInfo);
                body.add(fileBody);
                groupInfo.addTotalBytes(fileBody.contentLength());
            }
        }

        if (jsons != null && !jsons.isEmpty()) {
            for (String json : jsons) {
                ProgressRequestBody<String> fileBody = new StringProcess(json, "source", null, printUtil, groupInfo);
                body.add(fileBody);
                groupInfo.addTotalBytes(fileBody.contentLength());
            }
            // if the jsons size == 1, the data received by TM will be weird, adding an empty string helps TM receive the
            // proper data; the empty string should be dealt in TM.
            if (jsons.size() == 1) {
                ProgressRequestBody<String> fileBody = new StringProcess("", "source", null, printUtil, groupInfo);
                body.add(fileBody);
                groupInfo.addTotalBytes(fileBody.contentLength());
            }
        }

        ProgressRequestBody<String> fileBody = new StringProcess(String.valueOf(latest), "source", null, printUtil, groupInfo);
        body.add(fileBody);
        groupInfo.addTotalBytes(fileBody.contentLength());

        return body;
    }


    protected void uploadV2(String url, String fileName, ProcessGroupInfo groupInfo, List<ProgressRequestBody<?>> body, String connectionType, String signature) {
        OkHttpClient client = new OkHttpClient.Builder()
                .protocols(Arrays.asList(Protocol.HTTP_1_1))
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .build();

        MyCloudMultipartBody.Builder builder = new MyCloudMultipartBody.Builder("00content0boundary00");
        builder.setType(MultipartBody.FORM);

        for (ProgressRequestBody<?> requestBody : body) {
            builder.addFormDataPart(requestBody.name(), requestBody.fileName(), requestBody);
        }

        MyCloudMultipartBody requestBody = builder.build();
        Request request = new Request.Builder()
                .url(url)
//                .addHeader("Signature", signature)
//                .addHeader("Content-Type", "multipart/form-data; boundary=00content0boundary00")
//                .addHeader("charset", "utf-8")
                .post(requestBody)
                .build();

        analyseResult(client, request, groupInfo, printUtil, fileName, connectionType);
    }

    protected void upload(String url, File file, Map<String, BufferedInputStream> inputStreamMap, List<String> jsons, String connectionType, String signature) {
        String fileName = file.getName();
        OkHttpClient client = new OkHttpClient.Builder()
                .protocols(Arrays.asList(Protocol.HTTP_1_1))
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .build();
        final AtomicBoolean lock = new AtomicBoolean(false);
        ProcessGroupInfo groupInfo = new ProcessGroupInfo(lock);
        MyCloudMultipartBody.Builder builder = new MyCloudMultipartBody.Builder("00content0boundary00");
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
                StringProcess fileBody = new StringProcess(json, "source", null, printUtil, groupInfo);
                groupInfo.addTotalBytes(fileBody.contentLength());
                builder.addFormDataPart("source", null, fileBody);
            }
            // if the jsons size == 1, the data received by TM will be weird, adding an empty string helps TM receive the
            // proper data; the empty string should be dealt in TM.
            if (jsons.size() == 1) {
                StringProcess fileBody = new StringProcess("", "source", null, printUtil, groupInfo);
                groupInfo.addTotalBytes(fileBody.contentLength());
                builder.addFormDataPart("source", null, fileBody);
            }
        }
        // whether replace the latest version
        String l = String.valueOf(latest);
        StringProcess fileBody = new StringProcess(l, "source", null, printUtil, groupInfo);
        groupInfo.addTotalBytes(fileBody.contentLength());
        builder.addFormDataPart("latest", null, fileBody);

        MyCloudMultipartBody requestBody = builder.build();
        Request request = new Request.Builder()
                .url(url)
                .addHeader("Content-Length", String.valueOf(groupInfo.totalByte))
                .addHeader("Signature", signature)
                .addHeader("Content-Type", "multipart/form-data; boundary=00content0boundary00")
                .addHeader("charset", "utf-8")
                .post(requestBody)
                .build();

        analyseResult(client, request, groupInfo, printUtil, fileName, connectionType);
    }
}
