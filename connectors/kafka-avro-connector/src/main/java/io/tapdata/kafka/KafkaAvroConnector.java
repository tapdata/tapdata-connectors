package io.tapdata.kafka;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.kafka.service.KafkaService;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.UUID;

/**
 * 标准 Kafka 连接器
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/19 14:21 Create
 */
@TapConnectorClass("spec_kafka_avro.json")
public class KafkaAvroConnector extends KafkaEnhancedCoreConnector {

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        connectionContext.getLog().info("Starting {}", PDK_ID);
        DataMap dataMap = connectionContext.getConnectionConfig();
        dataMap.put("schemaRegister", true);
        dataMap.put("registrySchemaType", "AVRO");
        isConnectorStarted(connectionContext, connectorContext -> {
            String firstConnectorId = (String) connectorContext.getStateMap().get("firstConnectorId");
            if (EmptyKit.isNull(firstConnectorId)) {
                firstConnectorId = UUID.randomUUID().toString().replace("-", "");
                connectorContext.getStateMap().put("firstConnectorId", firstConnectorId);
            }
            kafkaConfig = KafkaConfig.valueOf(connectionContext, firstConnectorId);
        });
        if (!(connectionContext instanceof TapConnectorContext)) {
            kafkaConfig = KafkaConfig.valueOf(connectionContext, "");
        }
        stopping.compareAndSet(true, false);
        kafkaService = new KafkaService(kafkaConfig, stopping);
    }

}
