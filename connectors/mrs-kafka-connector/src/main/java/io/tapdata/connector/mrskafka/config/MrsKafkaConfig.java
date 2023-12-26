package io.tapdata.connector.mrskafka.config;

import io.tapdata.common.MqConfig;

import java.util.Set;

public class MrsKafkaConfig extends MqConfig {
    private Set<String> kafkaRawTopics;
    private String kafkaSaslMechanism;
    private Boolean enableScript = false;
    private String script;

    private Boolean huaweiKerberos;

    private String huaweiKeytab;

    private String huaweiKrb5Conf;

    private String huaweiPrincipal;

    private String huaweiDomainName;

    private String huaweiZookeeperPrincipal;

    private String huaweiServiceName;


    public Boolean getHuaweiKerberos() {
        return huaweiKerberos;
    }

    public void setHuaweiKerberos(Boolean huaweiKerberos) {
        this.huaweiKerberos = huaweiKerberos;
    }

    public String getHuaweiZookeeperPrincipal() {
        return huaweiZookeeperPrincipal;
    }

    public void setHuaweiZookeeperPrincipal(String huaweiZookeeperPrincipal) {
        this.huaweiZookeeperPrincipal = huaweiZookeeperPrincipal;
    }

    public String getHuaweiKeytab() {
        return huaweiKeytab;
    }

    public void setHuaweiKeytab(String huaweiKeytab) {
        this.huaweiKeytab = huaweiKeytab;
    }

    public String getHuaweiKrb5Conf() {
        return huaweiKrb5Conf;
    }

    public void setHuaweiKrb5Conf(String huaweiKrb5Conf) {
        this.huaweiKrb5Conf = huaweiKrb5Conf;
    }

    public String getHuaweiPrincipal() {
        return huaweiPrincipal;
    }


    public void setHuaweiPrincipal(String huaweiPrincipal) {
        this.huaweiPrincipal = huaweiPrincipal;
    }

    public String getHuaweiServiceName() {
        return huaweiServiceName;
    }

    public void setHuaweiServiceName(String huaweiServiceName) {
        this.huaweiServiceName = huaweiServiceName;
    }

    public String getHuaweiDomainName() {
        return huaweiDomainName;
    }

    public void setHuaweiDomainName(String huaweiDomainName) {
        this.huaweiDomainName = huaweiDomainName;
    }


    public String getAuthCredentialsSource() {
        return authCredentialsSource;
    }

    public void setAuthCredentialsSource(String authCredentialsSource) {
        this.authCredentialsSource = authCredentialsSource;
    }

    public String getAuthUserName() {
        return authUserName;
    }

    public void setAuthUserName(String authUserName) {
        this.authUserName = authUserName;
    }

    public String getAuthPassword() {
        return authPassword;
    }

    public void setAuthPassword(String authPassword) {
        this.authPassword = authPassword;
    }

    private String authCredentialsSource;
    private String authUserName;
    private String authPassword;


    /**
     * kafka source (Consumer)
     */
    private Integer kafkaConsumerRequestTimeout = 0;
    private Boolean kafkaConsumerUseTransactional = false;
    private Integer kafkaMaxPollRecords = 0;
    private Integer kafkaPollTimeoutMS = 0;
    private Integer kafkaMaxFetchBytes = 0;
    private Integer kafkaMaxFetchWaitMS = 0;
    private Boolean kafkaIgnoreInvalidRecord = false;
    /**
     * kafka target (Producer)
     */
    private Integer kafkaProducerRequestTimeout = 0;
    private Boolean kafkaProducerUseTransactional = false;
    private Integer kafkaRetries = 0;
    private Integer kafkaBatchSize = 0;
    private String kafkaAcks = "-1";
    private Integer kafkaLingerMS = 0;
    private Integer kafkaDeliveryTimeoutMS = 0;
    private Integer kafkaMaxRequestSize = 0;
    private Integer kafkaMaxBlockMS = 0;
    private Integer kafkaBufferMemory = 0;
    private String kafkaCompressionType = "";
    private String kafkaPartitionKey = "";
    private Boolean kafkaIgnorePushError = false;


    public Set<String> getKafkaRawTopics() {
        return kafkaRawTopics;
    }

    public void setKafkaRawTopics(Set<String> kafkaRawTopics) {
        this.kafkaRawTopics = kafkaRawTopics;
    }

    public String getKafkaSaslMechanism() {
        return kafkaSaslMechanism;
    }

    public void setKafkaSaslMechanism(String kafkaSaslMechanism) {
        this.kafkaSaslMechanism = kafkaSaslMechanism;
    }

    public Integer getKafkaConsumerRequestTimeout() {
        return kafkaConsumerRequestTimeout;
    }

    public void setKafkaConsumerRequestTimeout(Integer kafkaConsumerRequestTimeout) {
        this.kafkaConsumerRequestTimeout = kafkaConsumerRequestTimeout;
    }

    public Boolean getKafkaConsumerUseTransactional() {
        return kafkaConsumerUseTransactional;
    }

    public void setKafkaConsumerUseTransactional(Boolean kafkaConsumerUseTransactional) {
        this.kafkaConsumerUseTransactional = kafkaConsumerUseTransactional;
    }

    public Integer getKafkaMaxPollRecords() {
        return kafkaMaxPollRecords;
    }

    public void setKafkaMaxPollRecords(Integer kafkaMaxPollRecords) {
        this.kafkaMaxPollRecords = kafkaMaxPollRecords;
    }

    public Integer getKafkaPollTimeoutMS() {
        return kafkaPollTimeoutMS;
    }

    public void setKafkaPollTimeoutMS(Integer kafkaPollTimeoutMS) {
        this.kafkaPollTimeoutMS = kafkaPollTimeoutMS;
    }

    public Integer getKafkaMaxFetchBytes() {
        return kafkaMaxFetchBytes;
    }

    public void setKafkaMaxFetchBytes(Integer kafkaMaxFetchBytes) {
        this.kafkaMaxFetchBytes = kafkaMaxFetchBytes;
    }

    public Integer getKafkaMaxFetchWaitMS() {
        return kafkaMaxFetchWaitMS;
    }

    public void setKafkaMaxFetchWaitMS(Integer kafkaMaxFetchWaitMS) {
        this.kafkaMaxFetchWaitMS = kafkaMaxFetchWaitMS;
    }

    public Boolean getKafkaIgnoreInvalidRecord() {
        return kafkaIgnoreInvalidRecord;
    }

    public void setKafkaIgnoreInvalidRecord(Boolean kafkaIgnoreInvalidRecord) {
        this.kafkaIgnoreInvalidRecord = kafkaIgnoreInvalidRecord;
    }

    public Integer getKafkaProducerRequestTimeout() {
        return kafkaProducerRequestTimeout;
    }

    public void setKafkaProducerRequestTimeout(Integer kafkaProducerRequestTimeout) {
        this.kafkaProducerRequestTimeout = kafkaProducerRequestTimeout;
    }

    public Boolean getKafkaProducerUseTransactional() {
        return kafkaProducerUseTransactional;
    }

    public void setKafkaProducerUseTransactional(Boolean kafkaProducerUseTransactional) {
        this.kafkaProducerUseTransactional = kafkaProducerUseTransactional;
    }

    public Integer getKafkaRetries() {
        return kafkaRetries;
    }

    public void setKafkaRetries(Integer kafkaRetries) {
        this.kafkaRetries = kafkaRetries;
    }

    public Integer getKafkaBatchSize() {
        return kafkaBatchSize;
    }

    public void setKafkaBatchSize(Integer kafkaBatchSize) {
        this.kafkaBatchSize = kafkaBatchSize;
    }

    public String getKafkaAcks() {
        return kafkaAcks;
    }

    public void setKafkaAcks(String kafkaAcks) {
        this.kafkaAcks = kafkaAcks;
    }

    public Integer getKafkaLingerMS() {
        return kafkaLingerMS;
    }

    public void setKafkaLingerMS(Integer kafkaLingerMS) {
        this.kafkaLingerMS = kafkaLingerMS;
    }

    public Integer getKafkaDeliveryTimeoutMS() {
        return kafkaDeliveryTimeoutMS;
    }

    public void setKafkaDeliveryTimeoutMS(Integer kafkaDeliveryTimeoutMS) {
        this.kafkaDeliveryTimeoutMS = kafkaDeliveryTimeoutMS;
    }

    public Integer getKafkaMaxRequestSize() {
        return kafkaMaxRequestSize;
    }

    public void setKafkaMaxRequestSize(Integer kafkaMaxRequestSize) {
        this.kafkaMaxRequestSize = kafkaMaxRequestSize;
    }

    public Integer getKafkaMaxBlockMS() {
        return kafkaMaxBlockMS;
    }

    public void setKafkaMaxBlockMS(Integer kafkaMaxBlockMS) {
        this.kafkaMaxBlockMS = kafkaMaxBlockMS;
    }

    public Integer getKafkaBufferMemory() {
        return kafkaBufferMemory;
    }

    public void setKafkaBufferMemory(Integer kafkaBufferMemory) {
        this.kafkaBufferMemory = kafkaBufferMemory;
    }

    public String getKafkaCompressionType() {
        return kafkaCompressionType;
    }


    public void setKafkaCompressionType(String kafkaCompressionType) {
        this.kafkaCompressionType = kafkaCompressionType;
    }

    public String getKafkaPartitionKey() {
        return kafkaPartitionKey;
    }

    public void setKafkaPartitionKey(String kafkaPartitionKey) {
        this.kafkaPartitionKey = kafkaPartitionKey;
    }

    public Boolean getKafkaIgnorePushError() {
        return kafkaIgnorePushError;
    }

    public void setKafkaIgnorePushError(Boolean kafkaIgnorePushError) {
        this.kafkaIgnorePushError = kafkaIgnorePushError;
    }

    public Boolean getEnableScript() {
        return enableScript;
    }

    public void setEnableScript(Boolean enableScript) {
        this.enableScript = enableScript;
    }

    public String getScript() {
        return script;
    }

    public void setScript(String script) {
        this.script = script;
    }
}
