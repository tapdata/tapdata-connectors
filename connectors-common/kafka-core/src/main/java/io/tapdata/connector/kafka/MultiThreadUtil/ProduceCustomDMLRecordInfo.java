package io.tapdata.connector.kafka.MultiThreadUtil;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class ProduceCustomDMLRecordInfo {
	KafkaProducer<byte[],byte[]> kafkaProducer;
	CountDownLatch countDownLatch;
	private AtomicLong insert;
	private AtomicLong update;
	private AtomicLong delete;
	WriteListResult<TapRecordEvent> listResult;


	public ProduceCustomDMLRecordInfo(KafkaProducer<byte[], byte[]> kafkaProducer, CountDownLatch countDownLatch, AtomicLong insert, AtomicLong update, AtomicLong delete, WriteListResult<TapRecordEvent> listResult) {
		this.kafkaProducer = kafkaProducer;
		this.countDownLatch = countDownLatch;
		this.insert=insert;
		this.update=update;
		this.delete=delete;
		this.listResult=listResult;
	}

	public AtomicLong getInsert() {
		return insert;
	}

	public void setInsert(AtomicLong insert) {
		this.insert = insert;
	}

	public AtomicLong getUpdate() {
		return update;
	}

	public void setUpdate(AtomicLong update) {
		this.update = update;
	}

	public AtomicLong getDelete() {
		return delete;
	}

	public void setDelete(AtomicLong delete) {
		this.delete = delete;
	}

	public WriteListResult<TapRecordEvent> getListResult() {
		return listResult;
	}

	public void setListResult(WriteListResult<TapRecordEvent> listResult) {
		this.listResult = listResult;
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
