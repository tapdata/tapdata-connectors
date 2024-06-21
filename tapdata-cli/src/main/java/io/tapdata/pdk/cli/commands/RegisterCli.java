package io.tapdata.pdk.cli.commands;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.cli.CommonCli;
import io.tapdata.pdk.cli.services.UploadFileService;
import io.tapdata.pdk.cli.services.UploadServiceWithProcess;
import io.tapdata.pdk.cli.services.Uploader;
import io.tapdata.pdk.cli.utils.PrintUtil;
import io.tapdata.pdk.core.connector.TapConnector;
import io.tapdata.pdk.core.connector.TapConnectorManager;
import io.tapdata.pdk.core.constants.DataSourceQCType;
import io.tapdata.pdk.core.tapnode.TapNodeContainer;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.IOUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@CommandLine.Command(
        description = "Push PDK jar file into TM",
        subcommands = MainCli.class
)
public class RegisterCli extends CommonCli {
    private static final String TAG = RegisterCli.class.getSimpleName();
    private PrintUtil printUtil;
    @CommandLine.Parameters(paramLabel = "FILE", description = "One or more pdk jar files")
    File[] files;

    @CommandLine.Option(names = {"-l", "--latest"}, required = false, defaultValue = "true", description = "whether replace the latest version")
    private boolean latest;

    @CommandLine.Option(names = {"-a", "--auth"}, required = false, description = "Provide auth token to register")
    private String authToken;

    @CommandLine.Option(names = {"-ak", "--accessKey"}, required = false, description = "Provide auth accessKey")
    private String ak;

    @CommandLine.Option(names = {"-sk", "--secretKey"}, required = false, description = "Provide auth secretKey")
    private String sk;

    @CommandLine.Option(names = {"-t", "--tm"}, required = true, description = "Tapdata TM url")
    private String tmUrl;

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "TapData cli help")
    private boolean helpRequested = false;

    @CommandLine.Option(names = {"-r", "--replace"}, required = false, description = "Replace Config file name")
    private String replaceName;

    @CommandLine.Option(names = {"-f", "--filter"}, required = false, description = "The list which are the Authentication types should not be skipped, if value is empty will register all connector. if it contains multiple, please separate them with commas")
    private String needRegisterConnectionTypes;

    @CommandLine.Option(names = {"-X", "--X"}, required = false, description = "Output detailed logs, true or false")
    private boolean showAllMessage = false;

    @CommandLine.Option(names = {"-p", "--P"}, required = false, description = "Need show upload speed of progress")
    private boolean showProcess = false;

    public Integer execute() throws Exception {
        long registerStart = System.currentTimeMillis();
        printUtil = new PrintUtil(showAllMessage);
        Uploader uploadService = showProcess ?
                new UploadServiceWithProcess(printUtil, tmUrl, ak, sk, authToken, latest)
                : new UploadFileService(printUtil, tmUrl, ak, sk, authToken, latest);
        List<String> filterTypes = generateSkipTypes();
        if (!filterTypes.isEmpty()) {
            printUtil.print(PrintUtil.TYPE.TIP, String.format("* Starting to register data sources, plan to skip data sources that are not within the registration scope.\n* The types of data sources that need to be registered are: %s", filterTypes));
        } else {
            printUtil.print(PrintUtil.TYPE.TIP, "Start registering data sources and plan to register all submitted data sources");
        }
        StringJoiner unUploaded = new StringJoiner("\n");
        files = getAllJarFile(files);
        try {
            try {
                CommonUtils.setProperty("refresh_local_jars", "true");
                PrintStream out = System.out;
                try {
                    System.setOut(new PrintStream(new ByteArrayOutputStream() {
                        @Override
                        public void write(int b) {
                            if (showAllMessage) {
                                super.write(b);
                            }
                        }
                    }));
                    loadJar(files, out, System.out);
                } finally {
                    System.setOut(out);
                    printUtil.print0("\rAll submitted connectors from file system load completed " + CommandLine.Help.Ansi.AUTO.string("@|bold,fg(22) ⎷ |@") + "\n");
                }

                try {
                    printUtil.print(PrintUtil.TYPE.INFO, "Register connector to: " + tmUrl);
                    for (File file : files) {
                        printUtil.print(PrintUtil.TYPE.APPEND, String.format("* Register Connector: %s  Starting", file.getName()));
                        List<String> jsons = new ArrayList<>();
                        TapConnector connector = TapConnectorManager.getInstance().getTapConnectorByJarName(file.getName());
                        Collection<TapNodeInfo> tapNodeInfoCollection = connector.getTapNodeClassFactory().getConnectorTapNodeInfos();
                        Map<String, InputStream> inputStreamMap = new HashMap<>();
                        boolean needUpload = true;
                        String connectionType = "";
                        for (TapNodeInfo nodeInfo : tapNodeInfoCollection) {
                            TapNodeSpecification specification = nodeInfo.getTapNodeSpecification();
                            String authentication = specification.getManifest().get("Authentication");
                            connectionType = authentication;
                            if (needSkip(authentication, filterTypes)) {
                                needUpload = false;
                                printUtil.print(PrintUtil.TYPE.IGNORE, String.format("... Skipped with (%s)", connectionType));
                                break;
                            }
                            needUpload = true;
                            String iconPath = specification.getIcon();
                            if (StringUtils.isNotBlank(iconPath)) {
                                InputStream is = nodeInfo.readResource(iconPath);
                                if (is != null) {
                                    inputStreamMap.put(iconPath, is);
                                }
                            }

                            JSONObject o = (JSONObject) JSON.toJSON(specification);
                            DataSourceQCType qcType = DataSourceQCType.parse(specification.getManifest().get("Authentication"));
                            qcType = (null == qcType) ? DataSourceQCType.Alpha : qcType;
                            o.put("qcType", qcType);

                            String pdkAPIVersion = specification.getManifest().get("PDK-API-Version");
                            int pdkAPIBuildNumber = CommonUtils.getPdkBuildNumer(pdkAPIVersion);
                            o.put("pdkAPIVersion", pdkAPIVersion);
                            o.put("pdkAPIBuildNumber", pdkAPIBuildNumber);
                            o.put("beta", "beta".equals(authentication));
                            String nodeType = null;
                            switch (nodeInfo.getNodeType()) {
                                case TapNodeInfo.NODE_TYPE_SOURCE:
                                    nodeType = "source";
                                    break;
                                case TapNodeInfo.NODE_TYPE_SOURCE_TARGET:
                                    nodeType = "source_and_target";
                                    break;
                                case TapNodeInfo.NODE_TYPE_TARGET:
                                    nodeType = "target";
                                    break;
                                case TapNodeInfo.NODE_TYPE_PROCESSOR:
                                    nodeType = "processor";
                                    break;
                                default:
                                    throw new IllegalArgumentException("Unknown node type " + nodeInfo.getNodeType());
                            }
                            o.put("type", nodeType);
                            // get the version info and group info from jar
                            o.put("version", nodeInfo.getNodeClass().getPackage().getImplementationVersion());
                            o.put("group", nodeInfo.getNodeClass().getPackage().getImplementationVendor());

                            TapNodeContainer nodeContainer = JSON.parseObject(IOUtils.toString(nodeInfo.readResource(nodeInfo.getNodeClass().getAnnotation(TapConnectorClass.class).value())), TapNodeContainer.class);
                            if (nodeContainer.getDataTypes() == null) {
                                try (InputStream dataTypeInputStream = this.getClass().getClassLoader().getResourceAsStream("default-data-types.json")) {
                                    if (dataTypeInputStream != null) {
                                        String dataTypesJson = org.apache.commons.io.IOUtils.toString(dataTypeInputStream, StandardCharsets.UTF_8);
                                        if (StringUtils.isNotBlank(dataTypesJson)) {
                                            TapNodeContainer container = InstanceFactory.instance(JsonParser.class).fromJson(dataTypesJson, TapNodeContainer.class);
                                            if (container != null && container.getDataTypes() != null)
                                                nodeContainer.setDataTypes(container.getDataTypes());
                                        }
                                    }
                                }
                            }
                            Map<String, Object> messsages = nodeContainer.getMessages();
                            String replacePath = null;//"replace_default";
                            Map<String, Object> replaceConfig = needReplaceKeyWords(nodeInfo, replacePath);
                            if (messsages != null) {
                                Set<String> keys = messsages.keySet();
                                for (String key : keys) {
                                    if (!key.equalsIgnoreCase("default")) {
                                        Map<String, Object> messagesForLan = (Map<String, Object>) messsages.get(key);
                                        if (messagesForLan != null) {
                                            Object docPath = messagesForLan.get("doc");
                                            if (docPath instanceof String) {
                                                String docPathStr = (String) docPath;
                                                if (!inputStreamMap.containsKey(docPathStr)) {
                                                    Optional.ofNullable(nodeInfo.readResource(docPathStr)).ifPresent(stream -> {
                                                        InputStream inputStream = stream;
                                                        if (null != replaceConfig) {
                                                            Scanner scanner = null;
                                                            try {
                                                                scanner = new Scanner(stream, "UTF-8");
                                                                StringBuilder docTxt = new StringBuilder();
                                                                while (scanner.hasNextLine()) {
                                                                    docTxt.append(scanner.nextLine()).append("\n");
                                                                }
                                                                String finalTxt = docTxt.toString();
                                                                for (Map.Entry<String, Object> entry : replaceConfig.entrySet()) {
                                                                    finalTxt = finalTxt.replaceAll(entry.getKey(), String.valueOf(entry.getValue()));
                                                                }
                                                                inputStream = new ByteArrayInputStream(finalTxt.getBytes(StandardCharsets.UTF_8));
                                                            } catch (Exception e) {
                                                            } finally {
                                                                try {
                                                                    if (null != scanner) scanner.close();
                                                                } catch (Exception ignore) {
                                                                }
                                                            }
                                                        }
                                                        inputStreamMap.put(docPathStr, inputStream);
                                                    });
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            o.put("expression", JSON.toJSONString(nodeContainer.getDataTypes()));
                            if (nodeContainer.getMessages() != null) {
                                o.put("messages", nodeContainer.getMessages());
                            }

                            io.tapdata.pdk.apis.TapConnector connector1 = (io.tapdata.pdk.apis.TapConnector) nodeInfo.getNodeClass().getConstructor().newInstance();
                            ConnectorFunctions connectorFunctions = new ConnectorFunctions();
                            TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
                            connector1.registerCapabilities(connectorFunctions, codecRegistry);

                            List<Capability> capabilities = connectorFunctions.getCapabilities();
                            DataMap dataMap = nodeContainer.getConfigOptions();
                            if (dataMap != null) {
                                List<Map<String, Object>> capabilityList = (List<Map<String, Object>>) dataMap.get("capabilities");

                                if (CollectionUtils.isNotEmpty(capabilityList)) {
                                    for (Map<String, Object> capabilityFromSpec : capabilityList) {
                                        String capabilityId = (String) capabilityFromSpec.get("id");
                                        if (capabilityId != null) {
                                            List<String> alternatives = (List<String>) capabilityFromSpec.get("alternatives");
                                            capabilities.add(Capability.create(capabilityId).alternatives(alternatives).type(Capability.TYPE_OTHER));
                                        }
                                    }
                                }

                                Map<String, Object> supportDDL = (Map<String, Object>) dataMap.get("supportDDL");
                                if (supportDDL != null) {
                                    List<String> ddlEvents = (List<String>) supportDDL.get("events");
                                    if (ddlEvents != null) {
                                        for (String ddlEvent : ddlEvents) {
                                            capabilities.add(Capability.create(ddlEvent).type(Capability.TYPE_DDL));
                                        }
                                    }
                                }

                            }
                            if (CollectionUtils.isNotEmpty(capabilities)) {
                                o.put("capabilities", capabilities);
                                if (null != dataMap) {
                                    dataMap.remove("capabilities");
                                }
                            }

                            Map<Class<?>, String> tapTypeDataTypeMap = codecRegistry.getTapTypeDataTypeMap();
                            o.put("tapTypeDataTypeMap", JSON.toJSONString(tapTypeDataTypeMap));
                            String jsonString = o.toJSONString();
                            jsons.add(jsonString);
                        }

                        if (!needUpload) {
                            unUploaded.add(String.format("\t- %s's source types is [%s]", file.getName(), connectionType));
                            continue;
                        }
                        if (file.isFile()) {
                            printUtil.print(PrintUtil.TYPE.INFO, " => uploading ");
                            uploadService.upload(inputStreamMap, file, jsons);
                            printUtil.print(PrintUtil.TYPE.INFO, String.format("* Register Connector: %s | (%s) Completed", file.getName(), connectionType));
                            Collection<String> strings = connector.associatingIds();
                            new Thread(() -> {
                                for (String id : strings) {
                                    try {
                                        TapConnectorManager.getInstance().releaseAssociateId(id);
                                    } catch (Exception e) {
                                        System.out.println(e.getMessage());
                                        printUtil.print(PrintUtil.TYPE.DEBUG, "Release associate" + id + " failed, message: " + e.getMessage());
                                    }
                                }
                            }).start();
                        } else {
                            printUtil.print(PrintUtil.TYPE.DEBUG, "File " + file + " doesn't exists");
                            printUtil.print(PrintUtil.TYPE.DEBUG, file.getName() + " registered failed");
                        }
                    }
                } finally {
                    if (unUploaded.toString().length() > 0) {
                        printUtil.print(PrintUtil.TYPE.DEBUG, String.format("[INFO] Some connector that are not in the scope are registered this time: \n%s\nThe data connector type that needs to be registered is: %s\n", unUploaded.toString(), filterTypes));
                    }
                }
            } finally {
                printUtil.print(PrintUtil.TYPE.INFO, "Register all connector cost time: " + String.format("%.2fs", ((System.currentTimeMillis() - registerStart) / 1000.00)));
            }
            System.exit(0);
        } catch (Throwable throwable) {
            printUtil.print(PrintUtil.TYPE.ERROR, throwable.getMessage());
            throwable.printStackTrace(System.out);
            if (showAllMessage) {
                CommonUtils.logError(TAG, "Start failed", throwable);
            }
            System.exit(-1);
        }
        return 0;
    }

    protected void load(List<File> f, PrintStream out, PrintStream newOut) {
        final AtomicBoolean over = new AtomicBoolean(false);
        new Thread(() -> {
            try {
                TapConnectorManager.getInstance().start(f);
            } finally {
                over.compareAndSet(false, true);
            }
        }).start();
        int a = 0;
        printUtil.print0("\n");
        while (!over.get()) {
            StringBuilder builder = new StringBuilder("\rLoading all submitted connectors from file system, please wait ");
            if (a > 3) {
                a = 0;
            }
            switch (a) {
                case 0: builder.append(CommandLine.Help.Ansi.AUTO.string("@|bold,fg(22) / |@")); break;
                case 1: builder.append(CommandLine.Help.Ansi.AUTO.string("@|bold,fg(22) - |@")); break;
                case 2: builder.append(CommandLine.Help.Ansi.AUTO.string("@|bold,fg(22) \\ |@")); break;
                case 3: builder.append(CommandLine.Help.Ansi.AUTO.string("@|bold,fg(22) | |@")); break;
            }
            a++;
            builder.append("\r");
            synchronized (this) {
                try {
                    System.setOut(out);
                    printUtil.print0(builder.toString());
                } finally {
                    System.setOut(newOut);
                }
            }
            try {
                Thread.sleep(500);
            } catch (Exception e) {}
        }
    }

    protected void loadJar(File[] f, PrintStream out, PrintStream newOut) {
        if (f.length / 20 > 1) {
            load(Arrays.asList(f), out, newOut);
            return;
        }
        List<List<File>> arr = spilt(f, 20);
        load(arr.get(0), out, newOut);
        new Thread(() -> {
            for (int index = 1; index < arr.size(); index++) {
                TapConnectorManager.getInstance().start(arr.get(index));
            }
        }).start();
    }

    protected List<List<File>> spilt(File[] f, int eachCount) {
        int size = f.length / eachCount + (f.length % eachCount > 0 ? 1 : 0);
        List<List<File>> arr = new ArrayList<>();
        int index = 0;
        for (int x = 0; x < size; x++) {
            List<File> fx = new ArrayList<>();
            arr.add(fx);
            for (int y = 0; y < eachCount; y++) {
                fx.add(f[index]);
                index++;
            }
        }
        return arr;
    }

    public File[] getAllJarFile(File[] paths) {
        Collection<File> allJarFiles = getAllJarFiles(paths);
        File[] strings = new File[allJarFiles.size()];
        return allJarFiles.toArray(strings);
    }

    protected Collection<File> getAllJarFiles(File[] paths) {
        List<File> path = new ArrayList<>();
        for (File s : paths) {
            fileTypeDirector(s, path);
        }
        return path;
    }

    protected void fileTypeDirector(File f, List<File> pathSet) {
        int i = fileType(f);
        switch (i) {
            case 1:
                File[] files = f.listFiles();
                if (null != files && files.length > 0) {
                    pathSet.addAll(getAllJarFiles(files));
                }
                break;
            case 2:
                pathSet.add(f);
                break;
        }
    }


    protected int fileType(File file) {
        if (null == file || !file.exists()) {
            return -1;
        }
        if (file.isDirectory()) {
            return 1;
        }
        if (file.isFile() && file.getAbsolutePath().endsWith(".jar")) {
            return 2;
        }
        return -1;
    }

    protected static final String path = "tapdata-cli/src/main/resources/replace/";
    private Map<String, Object> needReplaceKeyWords(TapNodeInfo nodeInfo, String replacePath){
        if (null != this.replaceName && !"".equals(replaceName.trim())){
            replacePath = replaceName;
        }
        if (null == replacePath || "".equals(replacePath.trim())) return null;
        try {
            InputStream as = FileUtils.openInputStream(new File(path + replacePath + ".json"));//nodeInfo.readResource((String) replacePath);
            return JSON.parseObject(as, StandardCharsets.UTF_8, LinkedHashMap.class);
        }catch (IOException e){}
        return null;
    }

    protected List<String> generateSkipTypes() {
        List<String> needRegisterConnectionTypesArray = new ArrayList<>();
        if (!StringUtils.isBlank(needRegisterConnectionTypes)) {
            DataSourceQCType[] values = DataSourceQCType.values();
            String[] split = needRegisterConnectionTypes.toUpperCase().split(",");
            for (String tag : split) {
                for (DataSourceQCType value : values) {
                    String name = value.name();
                    if (name.equalsIgnoreCase(tag)) {
                        needRegisterConnectionTypesArray.add(name);
                        break;
                    }
                }
            }
        }
        return needRegisterConnectionTypesArray;
    }

    protected boolean needSkip(String authentication, List<String> skipList) {
        return StringUtils.isNotBlank(authentication) && !skipList.isEmpty() && !skipList.contains(String.valueOf(authentication).toUpperCase());
    }
}
