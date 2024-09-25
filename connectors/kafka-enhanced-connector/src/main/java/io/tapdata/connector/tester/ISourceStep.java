package io.tapdata.connector.tester;

import io.tapdata.connector.IConfigWithContext;
import io.tapdata.pdk.apis.entity.TestItem;

/**
 * 源测试项
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/28 18:59 Create
 */
public interface ISourceStep<C extends IConfigWithContext> extends IStep<C> {

    default boolean testRead(TestItem item) {
        return CHECK_ITEM_APPLY;
    }

    default boolean testReadLog(TestItem item) {
        return CHECK_ITEM_APPLY;
    }

    @Override
    default boolean test() {
        return checkItem(TestItem.ITEM_READ, this::testRead)
            && checkItem(TestItem.ITEM_READ_LOG, this::testReadLog);
    }
}
