package io.tapdata.connector.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2025-04-14 17:57
 **/
@TapExClass(
		code = 40,
		module = "kafka_enhanced",
		describe = "Kafka Error Codes",
		prefix = "KAFKA"
)
public interface KafkaErrorCodes {
	@TapExCode(
			describe = "Invalid Topic name",
			describeCN = "错误的Topic名称",
			solution = "Please check whether it is legal based on the following conditions\n - Illegal characters: /,\\, space, #, *,,?,',\":,;, @,!, $,%, %, ^, &, (,), +, =, {,}, [,], <,>, ~" +
					"\n - Length limit: The maximum length of the Topic name is 249 characters\n - Forbidden name: The Topic name cannot be . or ..\n - Case sensitive: The Topic name is case sensitive, for example, MyTopic and mytopic are considered different Topics" +
					"\n - Internal Topic Naming: Topic names starting with double underscore __ (such as __consumer_offsets) are usually regarded as internal use by Kafka, and user-defined use is not recommended." +
					"\n - Avoid mixing . and _: Since Kafka replaces . with _ when naming metrics internally, using these two characters can lead to naming conflicts",
			solutionCN = "请根据以下条件检查是否合法\n - 非法字符：/、\\、空格、#、*、,、?、'、\"、:、;、@、!、$、%、^、&、(、)、+、=、{、}、[、]、<、>、~" +
					"\n - 长度限制：Topic 名称的最大长度为 249 个字符\n - 禁止名称：Topic 名称不能为 . 或 ..\n - 大小写敏感：Topic 名称是大小写敏感的，例如 MyTopic 和 mytopic 被视为不同的 Topic" +
					"\n - 内部 Topic 命名：以双下划线 __ 开头的 Topic 名称（如 __consumer_offsets）通常被 Kafka 视为内部使用，不建议用户自定义使用" +
					"\n - 避免混用 . 和 _：由于 Kafka 在内部监控指标命名时会将 . 替换为 _，因此同时使用这两个字符可能导致命名冲突",
			dynamicDescription = "The topic currently used: {}",
			dynamicDescriptionCN = "当前使用的topic：{}"
	)
	String INVALID_TOPIC = "40001";

	@TapExCode(
			describe = "Unsupported event types",
			describeCN = "不支持的事件类型",
			dynamicDescription = "Event type: {}",
			dynamicDescriptionCN = "事件类型: {}"
	)
	String DEBEZIUM_NOT_SUPPORT_EVENT = "40002";
}
