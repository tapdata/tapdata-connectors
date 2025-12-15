package io.tapdata.kafka.service;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.tapdata.constant.MqTestItem;
import io.tapdata.entity.logger.Log;
import io.tapdata.kafka.IKafkaAdminService;
import io.tapdata.kafka.KafkaConfig;
import io.tapdata.pdk.apis.entity.TestItem;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Kafka 管理接口
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/29 14:04 Create
 */
public class KafkaAdminService implements IKafkaAdminService {
    private final Log logger;
    private final KafkaConfig config;
    private AdminClient adminClient;

    public KafkaAdminService(KafkaConfig config, Log logger) {
        this.logger = logger;
        this.config = config;
    }

    public synchronized AdminClient getAdminClient() {
        if (null == adminClient) {
            this.adminClient = AdminClient.create(config.buildAdminConfig());
        }
        return adminClient;
    }

    @Override
    public boolean isClusterConnectable() throws ExecutionException, InterruptedException {
        DescribeClusterResult describeClusterResult = getAdminClient().describeCluster();
        return describeClusterResult.controller().get() != null;
    }

    @Override
    public Set<String> listTopics() throws ExecutionException, InterruptedException {
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(false);
        ListTopicsResult listTopicsResult = getAdminClient().listTopics(options);
        KafkaFuture<Set<String>> future = listTopicsResult.names();
        return future.get();
    }

    @Override
    public void createTopic(Collection<String> topics, int numPartitions, short replicationFactor, CreateTopicsOptions createTopicsOptions) throws ExecutionException, InterruptedException {
        Collection<NewTopic> newTopics = new ArrayList<>();
        topics.forEach(topic -> newTopics.add(new NewTopic(topic, numPartitions, replicationFactor)));

        int interval = 5;
        boolean firstError = true;
        // 主题删除有延迟，需要用检查来保证全部被删除，检查 60 秒
        long endTime = System.currentTimeMillis() + 60 * 1000;
        while (System.currentTimeMillis() < endTime) {
            if (null == createTopicsOptions) {
                createTopicsOptions = new CreateTopicsOptions();
            }
            CreateTopicsResult createTopicsResult = getAdminClient().createTopics(newTopics, createTopicsOptions);
            for (Map.Entry<String, KafkaFuture<Void>> entry : createTopicsResult.values().entrySet()) {
                String topic = entry.getKey();
                KafkaFuture<Void> future = entry.getValue();
                try {
                    future.get(); //执行
                    logger.info("Created topic '{}': {}", topic, !future.isCompletedExceptionally());
                    return;
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof TopicExistsException) {
                        if (firstError) {
                            logger.warn("Recreate after {} seconds, because the topic {} is not deleted", interval, topic);
                            firstError = false;
                        }
                        Thread.sleep(interval);
                    } else {
                        throw e;
                    }
                }
            }
        }
    }

    @Override
    public void dropTopics(Collection<String> topics) throws ExecutionException, InterruptedException {
        List<String> deleteTopics = new ArrayList<>();
        Set<String> existTopics = listTopics();
        for (String topic : topics) {
            if (existTopics.contains(topic)) {
                deleteTopics.add(topic);
            } else {
                logger.info("Ignore delete because not exist topic '{}'", topic);
            }
        }

        DeleteTopicsResult deleteTopicsResult = getAdminClient().deleteTopics(deleteTopics);
        for (Map.Entry<String, KafkaFuture<Void>> entry : deleteTopicsResult.values().entrySet()) {
            entry.getValue().get(); //执行
        }

        // 主题删除有延迟，需要用检查来保证全部被删除，检查 30 秒
        long endTime = System.currentTimeMillis() + 30 * 1000;
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(false);
        do {
            boolean exists = false;
            ListTopicsResult listTopicsResult = getAdminClient().listTopics(options);
            KafkaFuture<Set<String>> future = listTopicsResult.names();
            for (String topic : future.get()) {
                if (topics.contains(topic)) {
                    exists = true;
                    break;
                }
            }
            if (!exists) break;
            Thread.sleep(500);
        } while (System.currentTimeMillis() < endTime);
    }

    @Override
    public void increaseTopicPartitions(String topic, Integer numPartitions) throws ExecutionException, InterruptedException {
        NewPartitions newPartitions = NewPartitions.increaseTo(numPartitions);
        Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
        newPartitionsMap.put(topic, newPartitions);
        CreatePartitionsResult createPartitionsResult = getAdminClient().createPartitions(newPartitionsMap);
        for (Map.Entry<String, KafkaFuture<Void>> entry : createPartitionsResult.values().entrySet()) {
            KafkaFuture<Void> future = entry.getValue();
            future.get(); //执行
            logger.info("Increase topic '{}' partitions({}): {}", entry.getKey(), numPartitions, !future.isCompletedExceptionally());
        }
    }

    @Override
    public List<TopicPartitionInfo> getTopicPartitionInfo(String topic) throws ExecutionException, InterruptedException {
        DescribeTopicsResult describeTopicsResult = getAdminClient().describeTopics(Collections.singleton(topic));
        Map<String, KafkaFuture<TopicDescription>> values = describeTopicsResult.values();
        KafkaFuture<TopicDescription> topicDescriptionKafkaFuture = values.get(topic);
        if (null != topicDescriptionKafkaFuture) {
            TopicDescription topicDescription = topicDescriptionKafkaFuture.get();
            return topicDescription.partitions();
        }
        return Collections.emptyList();
    }

    @Override
    public Collection<TopicPartition> getTopicPartitions(Collection<String> topics) throws ExecutionException, InterruptedException {
        DescribeTopicsResult describeTopicsResult = getAdminClient().describeTopics(topics);
        Map<String, KafkaFuture<TopicDescription>> values = describeTopicsResult.values();

        List<TopicPartition> partitionInfos = new ArrayList<>();
        for (Map.Entry<String, KafkaFuture<TopicDescription>> entry : values.entrySet()) {
            String topic = entry.getKey();
            KafkaFuture<TopicDescription> future = entry.getValue();
            TopicDescription description = future.get();
            List<TopicPartitionInfo> partitions = description.partitions();
            for (TopicPartitionInfo partitionInfo : partitions) {
                partitionInfos.add(new TopicPartition(topic, partitionInfo.partition()));
            }
        }
        return partitionInfos;
    }

    public void testRegistryConnect(TestItem testItem) {
        testItem.setItem(MqTestItem.KAFKA_SCHEMA_REGISTER_CONNECTION.getContent());
        try {
            Map<String, String> schemaRegistryConfigs = new HashMap<>();
            if (config.getConnectionBasicAuth()) {
                String authCredentialsSource = config.getConnectionAuthCredentialsSource();
                schemaRegistryConfigs.put("basic.auth.credentials.source", authCredentialsSource);
                if ("USER_INFO".equals(authCredentialsSource)) {
                    schemaRegistryConfigs.put("basic.auth.user.info", config.getConnectionAuthUserName() + ":" + config.getConnectionAuthPassword());
                } else if ("SASL_INHERIT".equals(authCredentialsSource)) {
                    schemaRegistryConfigs.put("sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", config.getConnectionAuthUserName(), config.getConnectionAuthPassword()));
                }
            }
            config.getConnectionExtParams().forEach(param -> {
                if (param.get("key").startsWith("schema.registry.")) {
                    schemaRegistryConfigs.put(param.get("key"), param.get("val"));
                }
            });
            // SSL/TLS Truststore configuration for HTTPS connection
//            schemaRegistryConfigs.put("schema.registry.ssl.truststore.location", "SCHEMA_REGISTRY_SSL_TRUSTSTORE_LOCATION");
//            schemaRegistryConfigs.put("schema.registry.ssl.truststore.password", "SCHEMA_REGISTRY_SSL_TRUSTSTORE_PASSWORD");
//            schemaRegistryConfigs.put("schema.registry.ssl.keystore.location", "SCHEMA_REGISTRY_SSL_KEYSTORE_LOCATION");
//            schemaRegistryConfigs.put("schema.registry.ssl.keystore.password", "SCHEMA_REGISTRY_SSL_KEYSTORE_PASSWORD");
            CachedSchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(config.getConnectionSchemaRegisterUrl(), 100, schemaRegistryConfigs);
            schemaRegistryClient.getAllSubjects();
            testItem.setResult(TestItem.RESULT_SUCCESSFULLY);
            testItem.setInformation("Schema register connection successfully");
        } catch (Exception e) {
            testItem.setResult(TestItem.RESULT_FAILED);
            testItem.setInformation("Please check the service address. " + e.getMessage());
        }
    }

    @Override
    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
    }
}
