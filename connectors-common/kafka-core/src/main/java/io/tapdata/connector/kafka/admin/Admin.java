package io.tapdata.connector.kafka.admin;

import org.apache.kafka.common.TopicPartitionInfo;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public interface Admin extends AutoCloseable {

  boolean isClusterConnectable();

  Set<String> listTopics();

  void createTopics(Set<String> topics);

  void createTopics(String topic, int numPartitions, short replicationFactor) throws ExecutionException, InterruptedException;

  void increaseTopicPartitions(String topic,Integer numPartitions) throws ExecutionException, InterruptedException;

  List<TopicPartitionInfo> getTopicPartitionInfo(String topic) throws ExecutionException, InterruptedException;

}
