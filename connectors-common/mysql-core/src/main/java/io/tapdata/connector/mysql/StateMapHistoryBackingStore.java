package io.tapdata.connector.mysql;

import io.debezium.config.Configuration;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.*;
import org.apache.commons.collections4.CollectionUtils;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2022-05-24 11:20
 **/
public class StateMapHistoryBackingStore extends AbstractDatabaseHistory {
	private static final String TAG = StateMapHistoryBackingStore.class.getSimpleName();
	private final DocumentWriter writer = DocumentWriter.defaultWriter();
	private final DocumentReader reader = DocumentReader.defaultReader();
	private String serverName;

	private MysqlSchemaHistoryTransfer schemaHistoryTransfer;

	@Override
	public void configure(Configuration config, HistoryRecordComparator comparator, DatabaseHistoryListener listener, boolean useCatalogBeforeSchema) {
		super.configure(config, comparator, listener, useCatalogBeforeSchema);
		this.serverName = config.getString("database.history.connector.id");
		schemaHistoryTransfer = LockManager.getSchemaHistoryTransfer(serverName);
	}

	@Override
	protected void storeRecord(HistoryRecord record) throws DatabaseHistoryException {
		schemaHistoryTransfer.executeWithLock(null, () -> {
			try {
				Document document = record.document();
				String hrJson = writer.write(document);
				Set<String> schemaSet = schemaHistoryTransfer.getHistoryMap().computeIfAbsent(serverName, k -> new LinkedHashSet<>());
				schemaSet.add(hrJson);
				schemaHistoryTransfer.unSave();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
	}

	@Override
	protected void recoverRecords(Consumer<HistoryRecord> records) {
		Set<String> schemaSet = schemaHistoryTransfer.getHistoryMap().get(serverName);
		if (CollectionUtils.isEmpty(schemaSet)) return;
		schemaSet.forEach(schemaJson -> {
			Document document;
			try {
				document = reader.read(schemaJson);
			} catch (IOException e) {
				throw new RuntimeException("Recover schema history record, failed to convert json to document, json: " + schemaJson, e);
			}
			HistoryRecord historyRecord = new HistoryRecord(document);
			records.accept(historyRecord);
		});
	}

	@Override
	public boolean exists() {
		return schemaHistoryTransfer.getHistoryMap().containsKey(serverName) && CollectionUtils.isNotEmpty(schemaHistoryTransfer.getHistoryMap().get(serverName));
	}

	@Override
	public boolean storageExists() {
		return true;
	}
}
