package io.tapdata.connector.kafka.exception;

import io.tapdata.exception.TapExCode;

public  interface KafkaExCode_11{
	@TapExCode(
		describe = "Kafka custom parsing message body parsing failed, please write a custom message body in the correct format",
		describeCN = "Kafka自定义解析消息体解析失败，请按照正确的格式编写自定义消息体",
		seeAlso = {}
	)
	String KAFKA_CUSTOM_READ_PARSE = "11013";

	@TapExCode(
		describe = "Kafka custom message body writing failed, please follow the instructions to write the custom script correctly",
		describeCN = "Kafka自定义消息体写失败，请按照说明正确的编写自定义脚本",
		seeAlso = {"https://docs.tapdata.io/enterprise/user-guide/connect-database/"}
	)
	String KAFKA_CUSTOM_WRITE_PARSE = "11014";
}
