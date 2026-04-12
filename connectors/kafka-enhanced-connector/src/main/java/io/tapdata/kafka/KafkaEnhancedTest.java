package io.tapdata.kafka;

import io.tapdata.connector.error.KafkaErrorCodes;
import io.tapdata.constant.MqTestItem;
import io.tapdata.entity.logger.Log;
import io.tapdata.exception.TapCodeException;
import io.tapdata.kafka.service.KafkaAdminService;
import io.tapdata.kafka.utils.Krb5Util;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.exception.TapTestItemException;
import io.tapdata.util.NetUtil;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.tapdata.base.ConnectorBase.testItem;

public class KafkaEnhancedTest implements AutoCloseable {

    private KafkaConfig kafkaConfig;
    private KafkaAdminService kafkaAdminService;
    private Consumer<TestItem> consumer;
    protected Map<String, Supplier<Boolean>> testFunctionMap;

    public KafkaEnhancedTest(KafkaConfig kafkaConfig, Consumer<TestItem> consumer, Log tapLogger) {
        this.kafkaConfig = kafkaConfig;
        this.consumer = consumer;
        this.kafkaAdminService = new KafkaAdminService(kafkaConfig, tapLogger);
        testFunctionMap = new LinkedHashMap<>();
        testFunctionMap.put("testHostPort", this::testHostAndPort);
        if (kafkaConfig.useKerberos()) {
            testFunctionMap.put("testKerberos", this::testKerberos);
        }
        testFunctionMap.put("testConnect", this::testConnect);
//        testFunctionMap.put("testVersion", this::testVersion);
    }

    public Boolean testHostAndPort() {
        String[] hostAndPort = kafkaConfig.getConnectionClusterURI().split(",");
        int failedCount = 0;
        for (String hostAndPortItem : hostAndPort) {
            String[] strs = hostAndPortItem.split(":");
            if (strs.length != 2) {
                consumer.accept(testItem(MqTestItem.NAME_SERVER.getContent(), TestItem.RESULT_FAILED, "name server address is invalid!"));
                return false;
            } else {
                try {
                    NetUtil.validateHostPortWithSocket(strs[0], Integer.parseInt(strs[1]));
                } catch (IOException e) {
                    failedCount++;
                } catch (NumberFormatException e) {
                    consumer.accept(testItem(MqTestItem.NAME_SERVER.getContent(), TestItem.RESULT_FAILED, "name server address is invalid!"));
                    return false;
                }
            }
        }
        if (failedCount == 0) {
            consumer.accept(testItem(MqTestItem.NAME_SERVER.getContent(), TestItem.RESULT_SUCCESSFULLY));
        } else if (failedCount == hostAndPort.length) {
            consumer.accept(testItem(MqTestItem.NAME_SERVER.getContent(), TestItem.RESULT_FAILED, "all addresses of name server is down!"));
            return false;
        } else {
            consumer.accept(testItem(MqTestItem.NAME_SERVER.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "some addresses of name server is down!"));
        }
        return true;
    }

    public Boolean testKerberos() {
        try {
            Krb5Util.checkKDCDomainsBase64(kafkaConfig.getKrb5Conf());
            consumer.accept(new TestItem(MqTestItem.KAFKA_BASE64_CONNECTION.getContent(), TestItem.RESULT_SUCCESSFULLY, null));
            return true;
        } catch (Exception e) {
            consumer.accept(new TestItem(MqTestItem.KAFKA_BASE64_CONNECTION.getContent(), TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        }
    }

    public Boolean testConnect() {
        try {
            if (kafkaAdminService.isClusterConnectable()) {
                consumer.accept(new TestItem(MqTestItem.KAFKA_MQ_CONNECTION.getContent(), TestItem.RESULT_SUCCESSFULLY, null));
                return true;
            } else {
                consumer.accept(new TestItem(MqTestItem.KAFKA_MQ_CONNECTION.getContent(), TestItem.RESULT_FAILED, "cluster is not connectable"));
                return false;
            }
        } catch (Exception e) {
            consumer.accept(new TestItem(MqTestItem.KAFKA_MQ_CONNECTION.getContent(), new TapTestItemException(new TapCodeException(KafkaErrorCodes.KAFKA_COMMON_ERROR, e)), TestItem.RESULT_FAILED));
            return false;
        }
    }

    public Boolean testOneByOne() {
        for (Map.Entry<String, Supplier<Boolean>> entry : testFunctionMap.entrySet()) {
            Boolean res = entry.getValue().get();
            if (EmptyKit.isNotNull(res) && !res) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() {
        ErrorKit.ignoreAnyError(() -> kafkaAdminService.close());
    }
}
