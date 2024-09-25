package io.tapdata.connector.tester;

import io.tapdata.connector.IConfigWithContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * 测试步骤接口定义
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/9/5 18:52 Create
 */
public interface IStep<C extends IConfigWithContext> extends AutoCloseable {

    boolean CHECK_ITEM_APPLY = true;
    boolean CHECK_ITEM_SKIPPED = false;

    C config();

    Consumer<TestItem> itemConsumer();

    ConnectionOptions options();

    /**
     * 执行测试项
     *
     * @param item      测试项名称
     * @param predicate 测试逻辑
     * @return 是否应用测试结果
     */
    default boolean checkItem(String item, Predicate<TestItem> predicate) {
        TestItem testItem = new TestItem(item, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "unsupported");
        if (predicate.test(testItem)) {
            itemConsumer().accept(testItem);
            return TestItem.RESULT_FAILED != testItem.getResult();
        }
        return CHECK_ITEM_APPLY;
    }

    /**
     * 执行测试步骤
     *
     * @return 是否执行下一个步骤
     */
    boolean test();
}
