package io.tapdata.connector.mrskafka.config;

import io.tapdata.connector.mrskafka.util.MrsKerberosUtil;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static io.tapdata.connector.mrskafka.config.MrsProducerConfiguration.*;

public class MrsAbstractConfiguration implements MrsKafkaConfiguration {
    protected final MrsKafkaConfig mrsKafkaConfig;

    protected final String connectorId;

    final Map<String, Object> configMap = new HashMap<>();
    protected final boolean ignoreInvalidRecord;

    public MrsAbstractConfiguration(MrsKafkaConfig mrsKafkaConfig, String connectorId) {
        this.mrsKafkaConfig = mrsKafkaConfig;
        this.connectorId = connectorId;
        this.ignoreInvalidRecord = mrsKafkaConfig.getKafkaIgnoreInvalidRecord();
    }

    @Override
    public Map<String, Object> build() {
        if (mrsKafkaConfig.getHuaweiKerberos()) {
            configMap.put("sasl.mechanism", "GSSAPI");
            configMap.put(SECURITY_PROTOCOL, "SASL_PLAINTEXT");
            configMap.put(SASL_KERBEROS_SERVICE_NAME, mrsKafkaConfig.getHuaweiServiceName());
            configMap.put(KERBEROS_DOMAIN_NAME, mrsKafkaConfig.getHuaweiDomainName());
            String parentDir = "connections-" + connectorId;
            String krb5Path = MrsKerberosUtil.saveByCatalog(parentDir, mrsKafkaConfig.getHuaweiKeytab(), mrsKafkaConfig.getHuaweiKrb5Conf(), true);
            String keytabPath = MrsKerberosUtil.keytabPath(krb5Path);
            MrsKerberosUtil.setKrb5Config(MrsKerberosUtil.confPath(krb5Path));
            MrsKerberosUtil.setZookeeperServerPrincipal(mrsKafkaConfig.getHuaweiZookeeperPrincipal());
            MrsKerberosUtil.setJaasFile(parentDir, mrsKafkaConfig.getHuaweiPrincipal(), keytabPath);
        }
        return configMap;
    }

    @Override
    public Set<String> getRawTopics() {
        return null;
    }

    @Override
    public Pattern getPatternTopics() {
        return null;
    }

    @Override
    public boolean hasRawTopics() {
        return false;
    }

    @Override
    public Duration getPollTimeout() {
        return null;
    }

    @Override
    public boolean isIgnoreInvalidRecord() {
        return false;
    }

    @Override
    public MrsKafkaConfig getConfig() {
        return null;
    }
}
