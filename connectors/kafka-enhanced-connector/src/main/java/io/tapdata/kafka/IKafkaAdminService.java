package io.tapdata.kafka;

import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Kafka 管理接口
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/29 12:17 Create
 */
public interface IKafkaAdminService extends AutoCloseable {

    boolean isClusterConnectable() throws ExecutionException, InterruptedException;

    Set<String> listTopics() throws ExecutionException, InterruptedException;

    default void createTopic(Collection<String> topics, int numPartitions, short replicationFactor) throws ExecutionException, InterruptedException {
        createTopic(topics, numPartitions, replicationFactor, null);
    }

    void createTopic(Collection<String> topics, int numPartitions, short replicationFactor, CreateTopicsOptions createTopicsOptions) throws ExecutionException, InterruptedException;

    void dropTopics(Collection<String> topics) throws ExecutionException, InterruptedException;

    void increaseTopicPartitions(String topic, Integer numPartitions) throws ExecutionException, InterruptedException;

    List<TopicPartitionInfo> getTopicPartitionInfo(String topic) throws ExecutionException, InterruptedException;

    Collection<TopicPartition> getTopicPartitions(Collection<String> topics) throws ExecutionException, InterruptedException;
}
