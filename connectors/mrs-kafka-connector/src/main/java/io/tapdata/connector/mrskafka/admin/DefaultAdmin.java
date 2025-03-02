package io.tapdata.connector.mrskafka.admin;

import io.tapdata.connector.mrskafka.config.MrsAdminConfiguration;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class DefaultAdmin implements Admin {
    private final AdminClient adminClient;

    public DefaultAdmin(MrsAdminConfiguration configuration) {
        this.adminClient = AdminClient.create(configuration.build());
    }

    @Override
    public boolean isClusterConnectable() {
        try {
            return adminClient.describeCluster().controller().get() != null;
        } catch (Throwable t) {
            throw new RuntimeException("fetch cluster controller error, " + t.getMessage(), t);
        }
    }

    @Override
    public Set<String> listTopics() {
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(false);
        try {
            return adminClient.listTopics(options).names().get();
        } catch (Throwable t) {
            throw new RuntimeException("fetch topic list error", t);
        }
    }

    @Override
    public void createTopics(Set<String> topics) {
        Set<NewTopic> newTopics = topics.stream()
                .map(topic -> new NewTopic(topic, 3, (short) 1))
                .collect(java.util.stream.Collectors.toSet());
        adminClient.createTopics(newTopics);
    }

    @Override
    public void createTopics(String topic, int numPartitions, short replicationFactor) throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic(topic, numPartitions, replicationFactor);
        adminClient.createTopics(Collections.singleton(newTopic)).all().get();
    }

    @Override
    public void increaseTopicPartitions(String topic, Integer numPartitions) throws ExecutionException, InterruptedException {
        NewPartitions newPartitions = NewPartitions.increaseTo(numPartitions);
        Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
        newPartitionsMap.put(topic, newPartitions);
        adminClient.createPartitions(newPartitionsMap).all().get();
    }

    @Override
    public List<TopicPartitionInfo> getTopicPartitionInfo(String topic) throws ExecutionException, InterruptedException {
        List<String> list = new ArrayList<>();
        list.add(topic);
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(list);
        Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
        TopicDescription topicDescription = stringTopicDescriptionMap.get(topic);
        List<TopicPartitionInfo> partitions = topicDescription.partitions();
        return partitions;
    }

    @Override
    public void close() throws Exception {
        if (adminClient != null) {
            adminClient.close();
        }
    }

    public AdminClient getAdminClient() {
        return adminClient;
    }
}
