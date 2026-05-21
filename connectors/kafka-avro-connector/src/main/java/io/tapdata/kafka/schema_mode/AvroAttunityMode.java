package io.tapdata.kafka.schema_mode;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.kafka.IKafkaService;

/**
 * {@link RegistryAvroMode} 的扩展范例：在父类生成的事件之上，把 Kafka 消息的元数据
 * （headers / partition / offset / timestamp）附加到 {@link TapEvent#getInfo()}，
 * 供下游做溯源或自定义路由。父类已实现的 schema diff、DDL 检测、主键 DDL 过滤等行为全部保留。
 * <p>
 * 作用域仅限本连接器实例，不影响同 JVM 内的其他 Kafka 连接器。
 */
public class AvroAttunityMode extends AvroEnhanceMode {

    public AvroAttunityMode(IKafkaService kafkaService) {
        super(kafkaService);
    }

}
