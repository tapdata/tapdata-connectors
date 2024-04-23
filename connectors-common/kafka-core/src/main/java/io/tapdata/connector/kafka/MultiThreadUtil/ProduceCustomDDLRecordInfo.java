package io.tapdata.connector.kafka.MultiThreadUtil;

import org.apache.kafka.clients.producer.KafkaProducer;
import java.util.concurrent.CountDownLatch;

public class ProduceCustomDDLRecordInfo {

	KafkaProducer<byte[],byte[]> kafkaProducer;
	CountDownLatch countDownLatch;

	public ProduceCustomDDLRecordInfo(KafkaProducer<byte[], byte[]> kafkaProducer, CountDownLatch countDownLatch) {
		this.kafkaProducer = kafkaProducer;
		this.countDownLatch = countDownLatch;
	}

	public KafkaProducer<byte[], byte[]> getKafkaProducer() {
		return kafkaProducer;
	}

	public void setKafkaProducer(KafkaProducer<byte[], byte[]> kafkaProducer) {
		this.kafkaProducer = kafkaProducer;
	}

	public CountDownLatch getCountDownLatch() {
		return countDownLatch;
	}

	public void setCountDownLatch(CountDownLatch countDownLatch) {
		this.countDownLatch = countDownLatch;
	}
}
