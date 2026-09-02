package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.write.PaimonTableWriteContext;
import io.tapdata.entity.logger.Log;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonServiceSpillRecoveryTest {

	@Test
	void spillChannelMissingFailureShouldBeRecoverable() throws Exception {
		PaimonService service = new PaimonService(new PaimonConfig(), mock(Log.class));
		Method method = PaimonService.class.getDeclaredMethod(
				"isRecoverableSpillFileFailure",
				Throwable.class);
		method.setAccessible(true);

		FileNotFoundException failure = new FileNotFoundException(
				"/tapdata_cache/paimon-io-test/abc.channel (No such file or directory)");
		Boolean recovered = (Boolean) method.invoke(service, new RuntimeException(failure));

		assertTrue(recovered);
	}

	@Test
	void recoverableSpillFailureShouldEvictBrokenWriteContext() throws Exception {
		PaimonService service = new PaimonService(new PaimonConfig(), mock(Log.class));
		String tableKey = "test_db.test_table";
		PaimonTableWriteContext context = mock(PaimonTableWriteContext.class);
		when(context.hasPendingCommit()).thenReturn(false);

		Field field = PaimonService.class.getDeclaredField("tableWriteContexts");
		field.setAccessible(true);
		@SuppressWarnings("unchecked")
		Map<String, PaimonTableWriteContext> contexts =
				(Map<String, PaimonTableWriteContext>) field.get(service);
		contexts.put(tableKey, context);

		Method method = PaimonService.class.getDeclaredMethod(
				"resetBrokenTableWriteContext",
				String.class,
				PaimonTableWriteContext.class,
				Log.class,
				Throwable.class);
		method.setAccessible(true);
		method.invoke(service, tableKey, context, mock(Log.class), new FileNotFoundException(
				"/tapdata_cache/paimon-io-test/abc.channel (No such file or directory)"));

		assertFalse(contexts.containsKey(tableKey));
		verify(context).close();
	}
}
