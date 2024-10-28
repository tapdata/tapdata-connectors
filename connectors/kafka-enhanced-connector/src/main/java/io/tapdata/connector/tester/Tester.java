package io.tapdata.connector.tester;

import io.tapdata.connector.IConfigWithContext;
import io.tapdata.constant.ConnectionTypeEnum;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * 连接器测试流程
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/28 16:05 Create
 */
public abstract class Tester<C extends IConfigWithContext> {

    protected C config;
    protected Consumer<TestItem> consumer;
    protected ConnectionOptions options;
    protected ConnectionTypeEnum connectionType;

    protected Tester(C config, Consumer<TestItem> consumer) {
        this.consumer = consumer;
        this.config = config;
        init();
    }

    protected void init() {
        options = new ConnectionOptions();
        connectionType = ConnectionTypeEnum.fromValue(Optional.ofNullable(config)
            .map(IConfigWithContext::tapConnectionContext)
            .map(TapConnectionContext::getConnectionConfig)
            .map(m -> (String) m.get("__connectionType"))
            .orElse(null)
        );
    }

    protected abstract ICommonStep<C> openCommon();

    protected abstract ISourceStep<C> openSource();

    protected abstract ITargetStep<C> openTarget();

    public Consumer<TestItem> getConsumer() {
        return consumer;
    }

    public C getConfig() {
        return config;
    }

    public ConnectionOptions getOptions() {
        return options;
    }

    public ConnectionTypeEnum getConnectionType() {
        return connectionType;
    }

    public ConnectionOptions start() throws Exception {
        try (ICommonStep<C> common = this.openCommon()) {
            if (!common.test()) {
                return getOptions();
            }
        }

        if (connectionType.hasSource()) {
            try (ISourceStep<C> source = this.openSource()) {
                if (!source.test()) return getOptions();
            }
        }

        if (connectionType.hasTarget()) {
            try (ITargetStep<C> target = this.openTarget()) {
                if (!target.test()) return getOptions();
            }
        }

        return getOptions();
    }
}
