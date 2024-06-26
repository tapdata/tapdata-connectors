package io.tapdata.connector.redis;

import io.tapdata.connector.redis.constant.DeployModeEnum;
import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.kit.EmptyKit;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.HostAndPort;

import java.net.URLEncoder;
import java.util.*;
import java.util.stream.Collectors;

public class RedisConfig {

    private static final BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class);

    private String host;
    private int port;
    private String username;
    private String password;
    private String deploymentMode;
    private String sentinelName;
    private int database;
    private ArrayList<LinkedHashMap<String, Integer>> sentinelAddress;
    private List<HostAndPort> clusterNodes;

    private String valueType = "List";
    private String keyExpression;
    private String keyPrefix;
    private String keyJoin;
    private String keySuffix;
    private String valueData = "Text";
    private String valueJoinString = ",";
    private String valueTransferredString = "";
    private Boolean csvFormat = true;
    private long expireTime;
    private Boolean resetExpire;
    private Boolean listHead = true;
    private Boolean oneKey = false;
    private String schemaKey = "-schema-key-";
    private long rateLimit = 5000L;

    private final static String DATA_BASE ="database";

    public RedisConfig load(Map<String, Object> map) {
        if (map != null && map.get(DATA_BASE) instanceof String) {
            map.put(DATA_BASE, Integer.valueOf(map.get(DATA_BASE).toString()));
        }
        beanUtils.mapToBean(map, this);
        if (EmptyKit.isNotNull(sentinelAddress)) {
            clusterNodes = sentinelAddress.stream().map(v ->
                    new HostAndPort(String.valueOf(v.get("host")), v.get("port"))).collect(Collectors.toList());
        }
        return this;
    }

    public String getReplicatorUri() {
        StringBuilder uri = new StringBuilder();
        switch (Objects.requireNonNull(DeployModeEnum.fromString(deploymentMode))) {
            case STANDALONE:
                uri.append("redis://").append(host).append(":").append(port);
                break;
            case SENTINEL:
                uri.append("redis-sentinel://");
                uri.append(clusterNodes.stream().map(v -> v.getHost() + ":" + v.getPort()).collect(Collectors.joining(",")));
                break;
            case CLUSTER:
                uri.append("redis://");
                uri.append(clusterNodes.stream().map(v -> v.getHost() + ":" + v.getPort()).collect(Collectors.joining(",")));
                break;
        }
        uri.append("?rateLimit=").append(rateLimit);
        if (StringUtils.isNotBlank(username)) {
            uri.append("&authUser=").append(username);
        }
        if (StringUtils.isNotBlank(password)) {
            uri.append("&authPassword=").append(URLEncoder.encode(password));
        }
        if (StringUtils.isNotBlank(sentinelName) && DeployModeEnum.fromString(deploymentMode) == DeployModeEnum.SENTINEL) {
            uri.append("&master=").append(sentinelName);
        }
        return uri.toString();
    }

    public String getReplicatorUri(HostAndPort node) {
        StringBuilder uri = new StringBuilder();
        uri.append("redis://").append(node.getHost()).append(":").append(node.getPort());
        uri.append("?rateLimit=").append(rateLimit);
        if (StringUtils.isNotBlank(username)) {
            uri.append("&authUser=").append(username);
        }
        if (StringUtils.isNotBlank(password)) {
            uri.append("&authPassword=").append(URLEncoder.encode(password));
        }
        return uri.toString();
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public String getDeploymentMode() {
        return deploymentMode;
    }

    public void setDeploymentMode(String deploymentMode) {
        this.deploymentMode = deploymentMode;
    }

    public String getSentinelName() {
        return sentinelName;
    }

    public void setSentinelName(String sentinelName) {
        this.sentinelName = sentinelName;
    }

    public ArrayList<LinkedHashMap<String, Integer>> getSentinelAddress() {
        return sentinelAddress;
    }

    public void setSentinelAddress(ArrayList<LinkedHashMap<String, Integer>> sentinelAddress) {
        this.sentinelAddress = sentinelAddress;
    }

    public List<HostAndPort> getClusterNodes() {
        return clusterNodes;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public String getValueType() {
        return valueType;
    }

    public void setValueType(String valueType) {
        this.valueType = valueType;
    }

    public long getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(long expireTime) {
        this.expireTime = expireTime;
    }

    public Boolean getResetExpire() {
        return resetExpire;
    }

    public void setResetExpire(Boolean resetExpire) {
        this.resetExpire = resetExpire;
    }

    public String getValueData() {
        return valueData;
    }

    public void setValueData(String valueData) {
        this.valueData = valueData;
    }

    public String getValueJoinString() {
        return valueJoinString;
    }

    public void setValueJoinString(String valueJoinString) {
        this.valueJoinString = valueJoinString;
    }

    public String getValueTransferredString() {
        return valueTransferredString;
    }

    public void setValueTransferredString(String valueTransferredString) {
        this.valueTransferredString = valueTransferredString;
    }

    public Boolean getCsvFormat() {
        return csvFormat;
    }

    public void setCsvFormat(Boolean csvFormat) {
        this.csvFormat = csvFormat;
    }

    public Boolean getListHead() {
        return listHead;
    }

    public void setListHead(Boolean listHead) {
        this.listHead = listHead;
    }

    public String getKeyExpression() {
        return keyExpression;
    }

    public void setKeyExpression(String keyExpression) {
        this.keyExpression = keyExpression;
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public void setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    public String getKeyJoin() {
        return keyJoin;
    }

    public void setKeyJoin(String keyJoin) {
        this.keyJoin = keyJoin;
    }

    public String getKeySuffix() {
        return keySuffix;
    }

    public void setKeySuffix(String keySuffix) {
        this.keySuffix = keySuffix;
    }

    public Boolean getOneKey() {
        return oneKey;
    }

    public void setOneKey(Boolean oneKey) {
        this.oneKey = oneKey;
    }

    public String getSchemaKey() {
        return schemaKey;
    }

    public void setSchemaKey(String schemaKey) {
        this.schemaKey = schemaKey;
    }

    public long getRateLimit() {
        return rateLimit;
    }

    public void setRateLimit(long rateLimit) {
        this.rateLimit = rateLimit;
    }
}
