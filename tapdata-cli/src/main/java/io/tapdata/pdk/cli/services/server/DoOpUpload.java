package io.tapdata.pdk.cli.services.server;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.tapdata.pdk.cli.services.ServiceUpload;
import io.tapdata.pdk.cli.services.request.FileProcess;
import io.tapdata.pdk.cli.services.request.InputStreamProcess;
import io.tapdata.pdk.cli.services.request.ProcessGroupInfo;
import io.tapdata.pdk.cli.utils.OkHttpUtils;
import io.tapdata.pdk.cli.utils.PrintUtil;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class DoOpUpload implements ServiceUpload {
    public static final String GENERATE_TOKEN_URL = "%s/api/users/generatetoken";
    public static final String UPLOAD_URL = "%s/api/pdk/upload/source?access_token=%s";

    boolean latest;
    String hostAndPort;
    String accessCode;
    PrintUtil printUtil;
    String token;

    public DoOpUpload(PrintUtil printUtil, String hp, String accessCode, boolean latest) {
        this.latest = latest;
        this.printUtil = printUtil;
        this.hostAndPort = fixUrl(hp);
        this.accessCode = accessCode;

        String tokenUrl = String.format(GENERATE_TOKEN_URL, hostAndPort);
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

    @Override
    public void upload(Map<String, BufferedInputStream> inputStreamMap, File file, List<String> jsons, String connectionType) {
        String url = String.format(UPLOAD_URL, hostAndPort, token);
        uploadOp(url, file, inputStreamMap, jsons, connectionType);
    }

    protected void uploadOp(String url, File file, Map<String, BufferedInputStream> inputStreamMap, List<String> jsons, String connectionType) {
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

        analyseResult(client, request, groupInfo, printUtil, fileName, connectionType);
    }
}
