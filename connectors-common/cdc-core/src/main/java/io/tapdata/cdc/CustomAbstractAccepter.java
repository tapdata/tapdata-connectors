package io.tapdata.cdc;

import io.tapdata.pdk.apis.consumer.TapStreamReadConsumer;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/10 15:03 Create
 * @description
 */
public abstract class CustomAbstractAccepter<EO, T extends CustomAbstractAccepter<EO, T, Consumer>, Consumer extends TapStreamReadConsumer<?, Object>>
        implements Acceptor<T, EO, Consumer> {
}
