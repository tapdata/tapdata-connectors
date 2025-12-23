package io.tapdata.mongodb.reader;

import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.pdk.apis.consumer.StreamReadOneByOneConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.List;

/**
 *
 */
public interface MongodbStreamReader {

		void onStart(MongodbConfig mongodbConfig);

		void read(TapConnectorContext connectorContext, List<String> tableList, Object offset, StreamReadOneByOneConsumer consumer) throws Exception;


		Object streamOffset(Long offsetStartTime);
		void onDestroy();
}
