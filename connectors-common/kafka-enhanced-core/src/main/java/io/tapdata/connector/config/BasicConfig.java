package io.tapdata.connector.config;

import io.tapdata.connector.IConfigWithContext;
import io.tapdata.pdk.apis.context.TapConnectionContext;

/**
 * 配置基类
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/28 14:13 Create
 */
public class BasicConfig implements IConfigWithContext {
    protected final TapConnectionContext context;

    public BasicConfig(TapConnectionContext context) {
        this.context = context;
    }

    @Override
    public TapConnectionContext tapConnectionContext() {
        return context;
    }

}
