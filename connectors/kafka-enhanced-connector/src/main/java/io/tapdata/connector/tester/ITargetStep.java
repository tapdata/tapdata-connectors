package io.tapdata.connector.tester;

import io.tapdata.connector.IConfigWithContext;
import io.tapdata.pdk.apis.entity.TestItem;

/**
 * 目标测试项
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/28 18:59 Create
 */
public interface ITargetStep<C extends IConfigWithContext> extends IStep<C> {

    default boolean testWrite(TestItem item) {
        return CHECK_ITEM_APPLY;
    }

    @Override
    default boolean test() {
        return checkItem(TestItem.ITEM_WRITE, this::testWrite);
    }
}
