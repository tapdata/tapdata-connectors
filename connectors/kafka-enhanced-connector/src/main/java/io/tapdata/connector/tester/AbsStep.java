package io.tapdata.connector.tester;

import io.tapdata.connector.IConfigWithContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.function.Consumer;

/**
 * 测试步骤
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/9/5 15:58 Create
 */
public abstract class AbsStep<C extends IConfigWithContext, T extends Tester<C>> implements IStep<C> {
    protected final T tester;

    protected AbsStep(T tester) {
        this.tester = tester;
    }

    @Override
    public C config() {
        return tester.getConfig();
    }

    @Override
    public Consumer<TestItem> itemConsumer() {
        return tester.getConsumer();
    }

    @Override
    public ConnectionOptions options() {
        return tester.getOptions();
    }
}
