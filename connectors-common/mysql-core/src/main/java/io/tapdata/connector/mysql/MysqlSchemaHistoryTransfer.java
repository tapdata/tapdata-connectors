package io.tapdata.connector.mysql;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

/**
 * @author samuel
 * @Description
 * @create 2022-05-25 16:10
 **/
public class MysqlSchemaHistoryTransfer {
	private   Map<String, Set<String>> historyMap = new ConcurrentHashMap<>();
	private AtomicBoolean saved = new AtomicBoolean(false);
	private ReentrantLock lock = new ReentrantLock(true);

	public void executeWithLock(Predicate<?> stop, Runner runner) {
		try {
			tryLock(stop);
			runner.execute();
		} finally {
			unLock();
		}
	}

	private void tryLock(Predicate<?> stop) {
		while (true) {
			if (null != stop && stop.test(null)) {
				break;
			}
			try {
				if (this.lock.tryLock(2, TimeUnit.MINUTES)) {
					break;
				}
			} catch (InterruptedException e) {
				break;
			}
		}
	}

	private void unLock() {
		this.lock.unlock();
	}

	public boolean isSave() {
		return this.saved.get();
	}

	public void unSave() {
		this.saved.set(false);
	}

	public Map<String, Set<String>> getHistoryMap() {
		return historyMap;
	}

	public void save() {
		this.saved.set(true);
	}

	public interface Runner {
		void execute();
	}
}
