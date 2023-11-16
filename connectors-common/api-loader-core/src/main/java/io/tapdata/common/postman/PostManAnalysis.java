package io.tapdata.common.postman;

import io.tapdata.common.postman.entity.ApiMap;
import io.tapdata.common.postman.entity.ApiVariable;
import io.tapdata.common.postman.entity.params.Api;
import io.tapdata.common.postman.entity.params.Body;
import io.tapdata.common.postman.entity.params.Header;
import io.tapdata.common.postman.entity.params.Url;
import io.tapdata.common.postman.enums.PostParam;
import io.tapdata.common.postman.util.ReplaceTagUtil;
import io.tapdata.common.support.APIFactory;
import io.tapdata.common.support.core.emun.TapApiTag;
import io.tapdata.common.support.entitys.APIResponse;
import io.tapdata.common.util.ScriptUtil;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.kit.ErrorKit;
import okhttp3.*;
import okio.Buffer;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.tapdata.base.ConnectorBase.*;

public class PostManAnalysis {
    private static final String TAG = PostManAnalysis.class.getSimpleName();
    private Map<String, Object> httpConfig;

    public void setHttpConfig(Map<String, Object> httpConfig) {
        this.httpConfig = httpConfig;
    }

    private Map<String, Object> connectorConfig;

    public void setConnectorConfig(Map<String, Object> connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    public void addConnectorConfig(Map<String, Object> connectorConfig) {
        if (Objects.isNull(this.connectorConfig)) {
            this.connectorConfig = new HashMap<>();
        }
        this.connectorConfig.putAll(connectorConfig);
    }

    public boolean filterUselessApi() {
        //是否过滤没有被标记的api
        return false;
    }

    public String sourcePath() {
        //默认的API JSON 存放位置。
        return APIFactory.DEFAULT_POST_MAN_FILE_PATH;
    }

    public static PostManAnalysis create() {
        return new PostManAnalysis();
    }

    private PostManApiContext apiContext;

    public PostManApiContext apiContext() {
        return this.apiContext;
    }

    public PostManAnalysis apiContext(PostManApiContext apiContext) {
        this.apiContext = apiContext;
        return this;
    }

    public Map<String, Object> getMap(String key, Map<String, Object> collection) {
        Object infoObj = collection.get(key);
        return (infoObj instanceof Map) ? ((Map<String, Object>) infoObj) : null;
    }

    public List<Object> getList(String key, Map<String, Object> collection) {
        Object infoObj = collection.get(key);
        return (infoObj instanceof Collection) ? (List<Object>) infoObj : null;
    }

    public Map<String, List<Object>> item(List<Object> item) {
        if (null == item) return null;
        List<Object> list = new ArrayList<>();
        put(list, item);

        return list.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(obj -> {
            Map<String, Object> request = (Map<String, Object>) ((Map<String, Object>) obj).get(PostParam.REQUEST);
            Object urlObj = request.get(PostParam.URL);
            String url = "UN_KNOW_URL";
            if (null != urlObj) {
                if (urlObj instanceof String) {
                    url = (String) urlObj;
                } else if (urlObj instanceof Map) {
                    url = String.valueOf(((Map<String, Object>) urlObj).get(PostParam.RAW));
                }
            }
            return ReplaceTagUtil.replace(url);
        }));
    }

    public void put(List<Object> list, Object obj) {
        if (null == obj) return;
        if (obj instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) obj;
            if (isMapFromItem(map)) {
                list.add(map);
            } else {
                map.forEach((key, value) -> put(list, value));
            }
        } else if (obj instanceof Collection) {
            Collection<Object> list1 = (Collection<Object>) obj;
            for (Object it : list1) {
                if (null == it) continue;
                put(list, it);
            }
        }
    }

    public boolean isMapFromItem(Object mapObj) {
        if (mapObj instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) mapObj;
            Object request = map.get(PostParam.REQUEST);
            return null != request;
        }
        return false;
    }

    public Request httpPrepare(String uriOrName, String method, Map<String, Object> params) {
        ApiMap.ApiEntity api = this.apiContext.apis().quickGet(uriOrName, method);
        if (Objects.isNull(api)) {
            throw new CoreException(String.format("No such api name or url is [%s],method is [%s]", uriOrName, method));
        }

        ApiVariable variable = ApiVariable.create();
        variable.putAll(this.apiContext.variable());
        if (Objects.nonNull(this.connectorConfig) && !this.connectorConfig.isEmpty()) {
            variable.putAll(this.connectorConfig);
        }
        if (Objects.nonNull(params) && !params.isEmpty()) {
            variable.putAll(params);
        }
        Map<String, Object> tempParam = new HashMap<>();
        tempParam.putAll(variable);
        //params.putAll(variable);

        //对url、body、header中的属性进行替换
        Api assignmentApi = api.variableAssignment(variable);

        //封装http

        io.tapdata.common.postman.entity.params.Request apiRequest = assignmentApi.request();
        Url apiUrl = apiRequest.url();
        String apiMethod = Objects.isNull(apiRequest.method()) ? "GET" : apiRequest.method().toUpperCase(Locale.ROOT);
        List<Header> apiHeader = apiRequest.header();
        Map<String, String> headMap = new HashMap<>();
        if (Objects.nonNull(apiHeader) && !apiHeader.isEmpty()) {
            apiHeader.stream().filter(ent -> {
                if (Objects.isNull(ent)) return false;
                return !ent.disabled();
            }).forEach(head -> headMap.put(head.key(), head.value()));
        }
        String url = apiUrl.raw();
        Body<?> apiBody = apiRequest.body();
        String contentType = apiBody.contentType();
        MediaType mediaType = MediaType.parse(contentType);
        Map<String, Object> bodyMap = new HashMap<>();
        String apiBodyContentType = apiBody.contentType();
        if ("application/json".equals(apiBodyContentType) && Objects.nonNull(apiBody.raw())) {
            try {
                bodyMap.putAll((Map<String, Object>) fromJson(String.valueOf(apiBody.raw())));
            } catch (Exception e) {
                TapLogger.debug(TAG, "API name is {} which body row cannot cast to a map, error row text: {}, Please ensure that the interface call uses the correct parameters. msg: {}", api.api().name(), apiBody.raw(), e.getMessage());
            }
        }
        List<Map<String, Object>> query = apiUrl.query();
        for (Map<String, Object> queryMap : query) {
            String key = String.valueOf(queryMap.get(PostParam.KEY));
            String desc = String.valueOf(queryMap.get(PostParam.DESCRIPTION));
            if (TapApiTag.isTapPageParam(desc)) {
                Object value = tempParam.get(key);
                if (Objects.nonNull(value)) {
                    queryMap.put(PostParam.VALUE, value);
                    //bodyMap.put(key, value);
                    String keyParam = key + "=";
                    if (url.contains(keyParam)) {
                        int indexOf = url.indexOf(keyParam);
                        int indexOfEnd = url.indexOf("&", indexOf);
                        String keyValueAgo = url.substring(indexOf, indexOfEnd < 0 ? url.length() : indexOfEnd);
                        url = url.replaceAll(keyValueAgo, keyParam + value);
                    }
                }
            }
        }

        bodyMap.putAll(params);
        Request.Builder builder = new Request.Builder()
                .url(url)
                .headers(Headers.of(headMap))
                .addHeader("Content-Type", contentType);
        if (!"GET".equals(apiMethod)) {
            builder.method(apiMethod, RequestBody.create(mediaType, apiBody.bodyJson(bodyMap)));
        } else {
            builder.get();
        }
        //apiBody.cleanCache();
        return builder.build();
    }
    OkHttpClient client;
    public APIResponse http(Request request) throws IOException {
        String property = System.getProperty("show_api_invoker_result", "1");
        if (!"1".equals(property)) {
            Buffer sink = new Buffer();
            RequestBody body = request.body();
            String bodyStr = "{}";
            if (Objects.nonNull(body)) {
                body.writeTo(sink);
                bodyStr = ScriptUtil.fileToString(sink.inputStream());
            }
            TapLogger.info(TAG, "Http Result: \tURL: {} \tMETHOD: {} \tBODY PARAMS: {}",
                    request.url(),
                    request.method(),
                    bodyStr
            );
        }
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        if (Objects.nonNull(client) && Objects.nonNull(client.connectionPool())){
            builder.connectionPool(client.connectionPool());
        }
        client = this.configHttp(builder).build();
        Call call = client.newCall(request);
        Map<String, Object> result = new HashMap<>();
        Response response = null;
        ResponseBody body = null;
        try {
            response = call.execute();
            if (null != response) {
                int code = response.code();
                Headers headers = response.headers();
                body = response.body();
                if (body != null) {
                    String bodyStr = body.string();
                    if (null != bodyStr) {
                        try {
                            result.put(Api.PAGE_RESULT_PATH_DEFAULT_PATH, fromJson(bodyStr));
                            return APIResponse.create()
                                    .httpCode(code)
                                    .result(result)
                                    .headers(getHeaderMap(headers));
                        } catch (Exception notMap) {
                            try {
                                result.put(Api.PAGE_RESULT_PATH_DEFAULT_PATH, fromJsonArray(bodyStr));
                                return APIResponse.create()
                                        .httpCode(code)
                                        .result(result)
                                        .headers(getHeaderMap(headers));
                            } catch (Exception notArray) {
                                result.put(Api.PAGE_RESULT_PATH_UN_KNOW_PATH, bodyStr);
                                result.put(Api.PAGE_RESULT_PATH_UN_KNOW_MSG, String.format("%s, %s", notMap.getMessage(), notArray.getMessage()));
                            }
                        }
                    }
                }
            }
            result.computeIfAbsent(Api.PAGE_RESULT_PATH_DEFAULT_PATH, key -> new HashMap<String, Object>());
            return APIResponse.create()
                    .httpCode(-1)
                    .result(result)
                    .headers(getHeaderMap(Headers.of()));
        } finally {
            if (null != response) {
                ErrorKit.ignoreAnyError(response::close);
            }
            if (null != body) {
                ErrorKit.ignoreAnyError(body::close);
            }
            if (call instanceof Closeable) {
                ErrorKit.ignoreAnyError(((Closeable) call)::close);
            }
        }
    }

    private OkHttpClient.Builder configHttp(OkHttpClient.Builder builder) {
        if (Objects.nonNull(this.httpConfig)) {
            try {
                int timeout = Integer.parseInt(String.valueOf(this.httpConfig.get("timeout")));
                builder.connectTimeout(timeout, TimeUnit.MILLISECONDS);
                builder.readTimeout(timeout, TimeUnit.MILLISECONDS);
            } catch (Exception ignored) {
            }
        }
        return builder;
    }

    public Map<String, Object> getHeaderMap(Headers headers) {
        if (headers == null) {
            return new HashMap<>();
        }
        Map<String, List<String>> multiMap = headers.toMultimap();
        //TODO
        return new HashMap<>();
    }

    public APIResponse http(String uriOrName, String method, Map<String, Object> params) {
        try {
            Request request = this.httpPrepare(uriOrName, method, params);
            APIResponse http = null;
            for (int i = 1; ; i++) {
                try {
                    http = this.http(request);
                    break;
                } catch (Exception e) {
                    if (i >= 3) {
                        throw e;
                    }
                    TapLogger.warn(TAG, "JS Connector exec postman api occur an error of method: "+ uriOrName + ", error: " + e.getMessage() + ", will retry 3 times, it's " + i);
                    Thread.sleep(100);
                }
            }
            String property = System.getProperty("show_api_invoker_result", "1");
            if (!"1".equals(property)) {
                TapLogger.info(TAG, "Http Result: Post Man: {} url - {}, method - {}, params - {}\n\t{}",
                        uriOrName,
                        request.url(),
                        method,
                        toJson(params),
                        toJson(http.result().get("data"), JsonParser.ToJsonFeature.PrettyFormat));
            }
            return http;
        } catch (Exception e) {
            throw new CoreException(String.format("Http request failed ,the api name or url is [%s],method is [%s], params are [%s], error message : %s", uriOrName, method, TapSimplify.toJson(params), e.getMessage()));
        }
    }

    public static boolean isPostMan(Map<String, Object> json) {
        return Objects.nonNull(json) && Objects.nonNull(json.get(PostParam.INFO)) && json.get(PostParam.INFO) instanceof Map && Objects.nonNull(((Map<String, Object>) json.get(PostParam.INFO)).get(PostParam._POSTMAN_ID));
    }
}
