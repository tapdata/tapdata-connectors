package io.tapdata.mongodb.reader;

import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.pdk.apis.consumer.TapStreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.List;

/**
 *
 */
public interface MongodbStreamReader {

		void onStart(MongodbConfig mongodbConfig);

	    MongodbStreamReader initAcceptor(int eventBatchSize, TapStreamReadConsumer<?, Object> consumer);

		void read(TapConnectorContext connectorContext, List<String> tableList, Object offset) throws Exception;

		Object streamOffset(Long offsetStartTime);
		void onDestroy();
}
