package io.tapdata.connector.rocketmq.config;

import io.tapdata.common.MqConfig;
import io.tapdata.kit.EmptyKit;

import java.io.Serializable;
import java.util.Map;

public class RocketmqConfig extends MqConfig implements Serializable {

    private String producerGroup = "tapdata";
    private String consumerGroup;
    private String produceTags;
    private String consumeExpression;
    private boolean useTLS;

    public RocketmqConfig load(Map<String, Object> map) {
        super.load(map);
        if (EmptyKit.isBlank(this.getNameSrvAddr())) {
            this.setNameSrvAddr(this.getMqHost() + ":" + this.getMqPort());
        }
        return this;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public boolean isUseTLS() {
        return useTLS;
    }

    public void setUseTLS(boolean useTLS) {
        this.useTLS = useTLS;
    }

    public String getProduceTags() {
        return produceTags;
    }

    public void setProduceTags(String produceTags) {
        this.produceTags = produceTags;
    }

    public String getConsumeExpression() {
        return consumeExpression;
    }

    public void setConsumeExpression(String consumeExpression) {
        this.consumeExpression = consumeExpression;
    }
}
